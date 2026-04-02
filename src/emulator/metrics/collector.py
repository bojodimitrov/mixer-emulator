from __future__ import annotations

import queue
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

from .runtime_metrics import RuntimeMetrics
from ..servers_config import METRICS_ENDPOINT
from ..transport_layer.transport import recv_message, send_message


class MetricsCollectorClient:
    def __init__(
        self,
        *,
        host: str = METRICS_ENDPOINT.host,
        port: int = METRICS_ENDPOINT.port,
        timeout_sec: float = 0.2,
        queue_size: int = 4096,
    ) -> None:
        self.host = str(host)
        self.port = int(port)
        self.timeout_sec = max(0.05, float(timeout_sec))
        self._queue: queue.Queue[Dict[str, Any]] = queue.Queue(
            maxsize=max(1, int(queue_size))
        )
        self._stop_event = threading.Event()
        self._sender_socket: Optional[socket.socket] = None
        self._sender_thread = threading.Thread(
            target=self._sender_loop,
            name="metrics-client-sender",
            daemon=True,
        )
        self._sender_thread.start()

    def record(self, target: str, ok: bool, latency_ms: float) -> None:
        msg = {
            "op": "record",
            "target": str(target),
            "ok": bool(ok),
            "latency_ms": float(latency_ms),
        }
        try:
            self._queue.put_nowait(msg)
        except queue.Full:
            return

    def record_error(self, source: str, message: str) -> None:
        msg = {
            "op": "error",
            "source": str(source),
            "message": str(message),
        }
        try:
            self._queue.put_nowait(msg)
        except queue.Full:
            return

    def close(self) -> None:
        self._stop_event.set()
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
        while not self._stop_event.is_set() or not self._queue.empty():
            try:
                msg = self._queue.get(timeout=0.1)
            except queue.Empty:
                continue

            try:
                self._send_record(msg)
            finally:
                self._queue.task_done()

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


class MetricsCollectorServer:
    def __init__(
        self,
        *,
        host: str = METRICS_ENDPOINT.host,
        port: int = METRICS_ENDPOINT.port,
        accept_timeout_sec: float = 1.0,
        conn_timeout_sec: float = 1.0,
        worker_pool_size: int = 8,
    ) -> None:
        self.host = str(host)
        self.port = int(port)
        self.accept_timeout_sec = max(0.1, float(accept_timeout_sec))
        self.conn_timeout_sec = max(0.1, float(conn_timeout_sec))
        self.worker_pool_size = max(1, int(worker_pool_size))

        self._metrics = RuntimeMetrics()
        self._socket: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._executor: Optional[ThreadPoolExecutor] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._executor = ThreadPoolExecutor(
            max_workers=self.worker_pool_size,
            thread_name_prefix="socket-metrics",
        )

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((self.host, self.port))
        self._socket.listen(64)
        self._socket.settimeout(self.accept_timeout_sec)

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._serve,
            name="socket-metrics",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        self._stop_event.set()

        if self._thread:
            self._thread.join(timeout=2.0)

        if self._socket:
            try:
                self._socket.close()
            except OSError:
                pass
            self._socket = None

        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None

    def snapshot(self) -> Dict[str, Any]:
        return self._metrics.snapshot()

    def _serve(self) -> None:
        assert self._socket is not None
        assert self._executor is not None

        while not self._stop_event.is_set():
            try:
                conn, _addr = self._socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            self._executor.submit(self._handle_connection, conn)

    def _handle_connection(self, connection: socket.socket) -> None:
        with connection:
            connection.settimeout(self.conn_timeout_sec)
            while not self._stop_event.is_set():
                try:
                    req = recv_message(connection)
                except (ConnectionError, OSError, socket.timeout):
                    return
                except Exception as exc:
                    send_message(connection, {"status": "error", "error": str(exc)})
                    continue

                if req.get("op") == "Close":
                    send_message(connection, {"status": "ok", "result": True})
                    return

                try:
                    resp = self._handle_message(req)
                except Exception as exc:
                    resp = {"status": "error", "error": str(exc)}
                send_message(connection, resp)

    def _handle_message(self, req: Dict[str, Any]) -> Dict[str, Any]:
        op = req.get("op")
        if op == "record":
            target = req.get("target")
            ok = bool(req.get("ok"))
            latency_ms = float(req.get("latency_ms") or 0.0)

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
            source = req.get("source")
            message = req.get("message")
            if not isinstance(source, str) or not isinstance(message, str):
                raise ValueError("error requires 'source' and 'message' strings")
            self._metrics.record_error(source, message)
            return {"status": "ok", "result": True}

        if op == "snapshot":
            return {"status": "ok", "result": self._metrics.snapshot()}

        raise ValueError("unknown metrics op")
