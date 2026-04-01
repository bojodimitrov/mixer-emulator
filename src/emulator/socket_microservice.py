import socket
import threading
from typing import Any, Dict, Optional

from .socket_config import DB_ENDPOINT, DEFAULT_SERVICE_ENDPOINT
from .service import Microservice, Request
from .transport import recv_message, send_message, bytes_from_hex, hex_from_bytes
from .storage.socket_client import SocketDatabaseClient


class SocketMicroserviceServer:
    """TCP server that exposes the existing `Microservice` API.

    Protocol (request):
      {"method": "GET", "hash": "<hex>"}
      {"method": "POST", "id": <int>, "new_name": "abcde"}

    Protocol (response):
      {"status": "ok", "result": ...}
      {"status": "error", "error": "..."}
    """

    def __init__(
        self,
        latency_ms: int = 50,
        pool_size: int = 200,
    ):
        self.host = str(DEFAULT_SERVICE_ENDPOINT.host)
        self.port = int(DEFAULT_SERVICE_ENDPOINT.port)
        self._db_client = SocketDatabaseClient(str(DB_ENDPOINT.host), int(DB_ENDPOINT.port))
        self._service = Microservice(
            db_client=self._db_client,
            latency_ms=latency_ms,
            pool_size=pool_size,
        )

        self._sock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        # If port=0 was used, OS picks an ephemeral port; publish it.
        self.port = int(self._sock.getsockname()[1])
        self._sock.listen(128)
        self._sock.settimeout(0.5)

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._serve, name="socket-service", daemon=True
        )
        self._thread.start()

    def close(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
        self._service.stop()

    def _serve(self) -> None:
        assert self._sock is not None
        while not self._stop_event.is_set():
            try:
                conn, _addr = self._sock.accept()
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
        if method == "GET":
            hash_hex = msg.get("hash")
            if not isinstance(hash_hex, str):
                raise ValueError("GET requires 'hash' hex string")
            payload = {"hash": bytes_from_hex(hash_hex)}
        elif method == "POST":
            id_ = msg.get("id")
            new_name = msg.get("new_name")
            if not isinstance(id_, int) or not isinstance(new_name, str):
                raise ValueError("POST requires 'id' int and 'new_name' str")
            payload = {"id": id_, "new_name": new_name}
        else:
            raise ValueError("unknown method")

        # Reuse existing Microservice queue/Request contract.
        import queue

        q: queue.Queue = queue.Queue()
        self._service.process(Request(method, payload, q))
        result = q.get(timeout=10)

        # Normalize bytes in response (if any) to hex to keep JSON clean.
        if result.get("status") == "ok" and isinstance(result.get("result"), tuple):
            # (id, name)
            rid, name = result["result"]
            result["result"] = [rid, name]
        return result


class SocketMicroserviceClient:
    """Frontend client for `SocketMicroserviceServer` over TCP."""

    def __init__(
        self,
        timeout_sec: float = 5.0,
    ):
        from .transport import TcpEndpoint

        self.endpoint = TcpEndpoint(DEFAULT_SERVICE_ENDPOINT.host, int(DEFAULT_SERVICE_ENDPOINT.port))
        self.timeout_sec = float(timeout_sec)

    def get(self, hash_bytes: bytes) -> Dict[str, Any]:
        with self.endpoint.connect(timeout_sec=self.timeout_sec) as socket:
            send_message(socket, {"method": "GET", "hash": hex_from_bytes(hash_bytes)})
            return recv_message(socket)

    def post(self, id_: int, new_name: str) -> Dict[str, Any]:
        with self.endpoint.connect(timeout_sec=self.timeout_sec) as sock:
            send_message(sock, {"method": "POST", "id": int(id_), "new_name": new_name})
            return recv_message(sock)
