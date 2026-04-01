import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

from ..transport_layer.transport import recv_message, send_message
from ..servers_config import DB_ENDPOINT
from .engine import DbEngine
from .orchestrator import DbRequest, DbOrchestrator, LookupStrategy


class DbServer:
    """TCP wrapper around `DbOrchestrator`.

    Protocol (request):
      {"op": "Query", "hash": "<hex>"}
      {"op": "Command", "id": <int>, "new_name": "abcde"}

    Protocol (response):
      {"status": "ok", "result": ...}
      {"status": "error", "error": "..."}
    """

    def __init__(
        self,
        lookup_strategy: LookupStrategy = DbEngine.STRATEGY_LINEAR,
        accept_timeout_sec: float = 0.5,
        conn_timeout_sec: float = 30.0,
        max_connections: int = 128,
        worker_pool_size: int = 32,
        tcp_nodelay: bool = True,
    ):
        self.endpoint = DB_ENDPOINT
        self.host = str(DB_ENDPOINT.host)
        self.port = int(DB_ENDPOINT.port)
        self._db_server = DbOrchestrator(lookup_strategy=lookup_strategy)

        self.accept_timeout_sec = float(accept_timeout_sec)
        self.conn_timeout_sec = float(conn_timeout_sec)
        self.max_connections = int(max_connections)
        self.worker_pool_size = int(worker_pool_size)
        self.tcp_nodelay = bool(tcp_nodelay)

        self._socket: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._executor: Optional[ThreadPoolExecutor] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        if self.worker_pool_size <= 0:
            raise ValueError("worker_pool_size must be positive")

        # One shared pool for connection handling, avoids thread-spawn overhead.
        self._executor = ThreadPoolExecutor(
            max_workers=self.worker_pool_size,
            thread_name_prefix="socket-db",
        )

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((self.host, self.port))
        self._socket.listen(self.max_connections)
        self._socket.settimeout(self.accept_timeout_sec)

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._serve, name="socket-db", daemon=True
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
        self._db_server.close()

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

            # Hand off to worker pool.
            self._executor.submit(self._handle_connection, conn)

    def _handle_connection(self, connection: socket.socket) -> None:
        with connection:
            if self.tcp_nodelay:
                try:
                    connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                except OSError:
                    pass

            connection.settimeout(self.conn_timeout_sec)

            # Keep-alive: process multiple request/response pairs on the same TCP connection.
            while not self._stop_event.is_set():
                try:
                    req = recv_message(connection)
                except (ConnectionError, OSError, socket.timeout):
                    # Client disconnected / idle timeout / socket died.
                    return
                except Exception as exc:
                    send_message(connection, {"status": "error", "error": str(exc)})
                    continue

                if req.get("op") == "Close":
                    send_message(connection, {"status": "ok", "result": True})
                    return

                try:
                    resp = self._dispatch(req)
                except Exception as exc:
                    resp = {"status": "error", "error": str(exc)}
                send_message(connection, resp)

    def _dispatch(self, req: Dict[str, Any]) -> Dict[str, Any]:
        op = req.get("op")
        if op == "Query":
            hash_hex = req.get("hash")
            if not isinstance(hash_hex, str):
                raise ValueError("Query requires 'hash' hex string")

            result = self._db_server.handle_request(
                DbRequest("Query", {"hash": hash_hex})
            )
            return {"status": "ok", "result": result}

        if op == "Command":
            id_ = req.get("id")
            new_name = req.get("new_name")
            if not isinstance(id_, int) or not isinstance(new_name, str):
                raise ValueError("Command requires 'id' int and 'new_name' str")
            result = self._db_server.handle_request(
                DbRequest("Command", {"id": id_, "new_name": new_name})
            )
            return {"status": "ok", "result": result}

        raise ValueError("unknown op")
