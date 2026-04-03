from __future__ import annotations

import queue
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

from .runtime_metrics import RuntimeMetrics
from ..servers_config import METRICS_ENDPOINT
from ..transport_layer.selector_json_server import SelectorJsonServer
from ..transport_layer.transport import recv_message, send_message


class MetricsCollectorClient:
    def __init__(self) -> None:
        self.host = str(METRICS_ENDPOINT.host)
        self.port = int(METRICS_ENDPOINT.port)

        self.timeout_sec = 0.2
        self._work_queue: queue.PriorityQueue[tuple[int, int, Any]] = (
            queue.PriorityQueue()
        )
        self._seq = 0
        self._seq_lock = threading.Lock()
        self._shutdown_token = object()
        self._stop_event = threading.Event()
        self._sender_socket: Optional[socket.socket] = None
        self._sender_thread = threading.Thread(
            target=self._sender_loop,
            name="metrics-client-sender",
            daemon=True,
        )
        self._sender_thread.start()

    def _next_seq(self) -> int:
        with self._seq_lock:
            self._seq += 1
            return self._seq

    def record(self, target: str, ok: bool, latency_ms: float) -> None:
        if self._stop_event.is_set():
            return

        msg = {
            "op": "record",
            "target": str(target),
            "ok": bool(ok),
            "latency_ms": float(latency_ms),
        }
        self._work_queue.put((1, self._next_seq(), msg))

    def record_error(self, source: str, message: str) -> None:
        if self._stop_event.is_set():
            return

        msg = {
            "op": "error",
            "source": str(source),
            "message": str(message),
        }
        # Errors are prioritized over normal records via lower priority value.
        self._work_queue.put((0, self._next_seq(), msg))

    def close(self) -> None:
        self._stop_event.set()

        shutdown = getattr(self._work_queue, "shutdown", None)
        if callable(shutdown):
            shutdown(immediate=False)
        else:
            self._work_queue.put((2, self._next_seq(), self._shutdown_token))

        if self._sender_thread.is_alive():
            self._sender_thread.join(timeout=1.0)
        self._drop_sender_socket()

    def _drop_sender_socket(self) -> None:
        if self._sender_socket is None:
            return
        try:
            self._sender_socket.close()
        except OSError:
            pass
        self._sender_socket = None

    def _get_sender_socket(self) -> socket.socket:
        if self._sender_socket is None:
            sock = socket.create_connection(
                (self.host, self.port), timeout=self.timeout_sec
            )
            sock.settimeout(self.timeout_sec)
            self._sender_socket = sock
        return self._sender_socket

    def _sender_loop(self) -> None:
        while True:
            if hasattr(queue, "ShutDown"):
                try:
                    _, _, msg = self._work_queue.get(block=True)
                except queue.ShutDown:  # type: ignore[attr-defined]
                    return
            else:
                _, _, msg = self._work_queue.get(block=True)

            if msg is self._shutdown_token:
                self._work_queue.task_done()
                return

            try:
                self._send_record(msg)
            finally:
                self._work_queue.task_done()

    def _send_record(self, msg: Dict[str, Any]) -> None:
        # Keep one sender connection open to avoid creating a socket per metric.
        for _ in range(2):
            try:
                sock = self._get_sender_socket()
                send_message(sock, msg)
                recv_message(sock)
                return
            except OSError:
                self._drop_sender_socket()

        # Metrics are best-effort and must not impact runtime behavior.
        return

    def _request_once(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        with socket.create_connection(
            (self.host, self.port), timeout=self.timeout_sec
        ) as sock:
            sock.settimeout(self.timeout_sec)
            send_message(sock, msg)
            return recv_message(sock)

    def snapshot(self) -> Dict[str, Any]:
        msg = {"op": "snapshot"}
        last_exc: Optional[Exception] = None
        resp: Dict[str, Any]
        for attempt in range(3):
            try:
                resp = self._request_once(msg)
                break
            except OSError as exc:
                last_exc = exc
                if attempt < 2:
                    time.sleep(0.05)
                continue
        else:
            raise RuntimeError("metrics snapshot failed after retries") from last_exc

        if resp.get("status") != "ok":
            raise RuntimeError(str(resp.get("error") or "metrics snapshot failed"))

        data = resp.get("result")
        if not isinstance(data, dict):
            raise RuntimeError("metrics snapshot payload was not an object")
        return data


class MetricsCollectorServer(SelectorJsonServer):
    def __init__(self) -> None:
        self._metrics = RuntimeMetrics()
        super().__init__(
            host=METRICS_ENDPOINT.host,
            port=int(METRICS_ENDPOINT.port),
            max_connections=64,
            worker_pool_size=16,
            accept_timeout_sec=1.0,
            conn_timeout_sec=10.0,
            thread_name="socket-metrics",
            worker_thread_prefix="socket-metrics",
        )

    def snapshot(self) -> Dict[str, Any]:
        return self._metrics.snapshot()

    def _handle_request_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        op = msg.get("op")
        if op == "record":
            target = msg.get("target")
            ok = bool(msg.get("ok"))
            latency_ms = float(msg.get("latency_ms") or 0.0)

            if target == "db":
                self._metrics.record_db(ok, latency_ms)
            elif target == "service":
                self._metrics.record_service(ok, latency_ms)
            elif target in {"corrupter", "repairer"}:
                self._metrics.record_client(target, ok, latency_ms)
            else:
                raise ValueError("invalid metrics target")

            return {"status": "ok", "result": True}

        if op == "error":
            source = msg.get("source")
            message = msg.get("message")
            if not isinstance(source, str) or not isinstance(message, str):
                raise ValueError("error requires 'source' and 'message' strings")
            self._metrics.record_error(source, message)
            return {"status": "ok", "result": True}

        if op == "snapshot":
            return {"status": "ok", "result": self._metrics.snapshot()}

        raise ValueError("unknown metrics op")

    def _record_error(self, message: str) -> None:
        self._metrics.record_error("metrics", str(message))

    def _record_metric(self, ok: bool, latency_ms: float) -> None:
        # Metrics server requests are not themselves metricated
        pass
