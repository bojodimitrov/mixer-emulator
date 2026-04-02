import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

from ..metrics.collector import MetricsCollectorClient
from ..servers_config import SERVICE_ENDPOINT
from ..metrics.runtime_metrics import now
from .framework import Microservice, Request
from ..transport_layer.transport import (
    recv_message,
    send_message,
)


class MicroserviceServer:
    """TCP server that exposes the existing `Microservice` API.

    Protocol (request):
        {"method": "GET/POST", "path": "/<endpoint_name>", "data": dictionary<str, object>

    Protocol (response):
        {"status": "ok", "result": ...}
        {"status": "error", "error": "..."}
    """

    def __init__(
        self,
        latency_ms: int = 50,
        pool_size: int = 200,
        connection_pool_size: int = 16,
        max_requests_per_connection: int = 8,
        accept_timeout_sec: float = 1.0,
        conn_timeout_sec: float = 10.0,
    ):
        self.host = str(SERVICE_ENDPOINT.host)
        self.port = int(SERVICE_ENDPOINT.port)
        self._service = Microservice(
            latency_ms=latency_ms,
            pool_size=pool_size,
        )

        self._socket: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._connection_pool_size = max(1, int(connection_pool_size))
        self.max_requests_per_connection = max(1, int(max_requests_per_connection))
        self.accept_timeout_sec = float(accept_timeout_sec)
        self.conn_timeout_sec = float(conn_timeout_sec)
        self._metrics_client: MetricsCollectorClient = MetricsCollectorClient()
        self._executor: Optional[ThreadPoolExecutor] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._executor = ThreadPoolExecutor(
            max_workers=self._connection_pool_size,
            thread_name_prefix="socket-service",
        )

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((self.host, self.port))
        self._socket.listen(128)
        self._socket.settimeout(self.accept_timeout_sec)

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._serve, name="socket-service", daemon=True
        )
        self._thread.start()

    def close(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)
        if self._socket:
            try:
                self._socket.close()
            except OSError:
                pass
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
        self._metrics_client.close()
        self._service.stop()

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

    def _handle_connection(self, conn: socket.socket) -> None:
        with conn:
            conn.settimeout(self.conn_timeout_sec)
            handled = 0
            while (
                not self._stop_event.is_set()
                and handled < self.max_requests_per_connection
            ):
                reply: Dict[str, Any]
                started = now()
                try:
                    msg = recv_message(conn)
                except (ConnectionError, OSError, socket.timeout):
                    return
                except Exception as exc:
                    self._metrics_client.record_error("service", str(exc))
                    send_message(conn, {"status": "error", "error": str(exc)})
                    continue

                if msg.get("op") == "Close":
                    send_message(conn, {"status": "ok", "result": True})
                    return

                try:
                    reply = self._handle_message(msg)
                except Exception as exc:
                    self._metrics_client.record_error("service", str(exc))
                    reply = {"status": "error", "error": str(exc)}
                finally:
                    self._emit_metrics(reply, started)
                send_message(conn, reply)
                handled += 1

    def _emit_metrics(self, resp: Dict[str, Any], started: float) -> None:
        ok = isinstance(resp, dict) and resp.get("status") == "ok"
        self._metrics_client.record("service", ok, (now() - started) * 1000.0)

    def _handle_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        method = (msg.get("method") or "").upper()
        path = msg.get("path") or "/"
        data = msg.get("data")

        if not isinstance(data, dict):
            raise ValueError("request 'data' must be an object")

        # Reuse existing Microservice queue/Request contract.
        import queue

        q = queue.Queue()
        self._service.process(Request(method, data, path, q))
        result = q.get(timeout=10)

        # Normalize bytes in response (if any) to hex to keep JSON clean.
        if result.get("status") == "ok" and isinstance(result.get("result"), tuple):
            # (id, name)
            rid, name = result["result"]
            result["result"] = [rid, name]

        return result
