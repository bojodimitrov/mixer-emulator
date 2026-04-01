import socket
import threading
from typing import Any, Dict, Optional

from ..servers_config import SERVICE_ENDPOINT
from .framework import Microservice, Request
from ..transport_layer.transport import (
    recv_message,
    send_message,
)
from ..storage.client import DbClient


class MicroserviceServer:
    """TCP server that exposes the existing `Microservice` API.

        Protocol (request):
            {"method": "GET", "data": {"hash": "<hex>"}}
            {"method": "POST", "data": {"id": <int>, "new_name": "abcde"}}

    Protocol (response):
      {"status": "ok", "result": ...}
      {"status": "error", "error": "..."}
    """

    def __init__(
        self,
        latency_ms: int = 50,
        pool_size: int = 200,
    ):
        self.host = str(SERVICE_ENDPOINT.host)
        self.port = int(SERVICE_ENDPOINT.port)
        self._db_client = DbClient()
        self._service = Microservice(
            db_client=self._db_client,
            latency_ms=latency_ms,
            pool_size=pool_size,
        )

        self._socket: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((self.host, self.port))
        self._socket.listen(128)
        self._socket.settimeout(0.5)

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
        self._service.stop()

    def _serve(self) -> None:
        assert self._socket is not None
        while not self._stop_event.is_set():
            try:
                conn, _addr = self._socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            threading.Thread(
                target=self._handle_conn,
                args=(conn,),
                name="socket-service-conn",
                daemon=True,
            ).start()

    def _handle_conn(self, conn: socket.socket) -> None:
        with conn:
            reply: Dict[str, Any]
            try:
                msg = recv_message(conn)
                reply = self._handle_message(msg)
            except Exception as exc:
                reply = {"status": "error", "error": str(exc)}
            send_message(conn, reply)

    def _handle_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        method = (msg.get("method") or "").upper()
        data = msg.get("data")

        if not isinstance(data, dict):
            raise ValueError("request 'data' must be an object")

        # Reuse existing Microservice queue/Request contract.
        import queue

        q: queue.Queue = queue.Queue()
        self._service.process(Request(method, data, q))
        result = q.get(timeout=10)

        # Normalize bytes in response (if any) to hex to keep JSON clean.
        if result.get("status") == "ok" and isinstance(result.get("result"), tuple):
            # (id, name)
            rid, name = result["result"]
            result["result"] = [rid, name]

        return result
