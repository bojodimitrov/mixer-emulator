import socket
from contextlib import suppress
from typing import Optional, Tuple

from ..socket_config import DEFAULT_DB_ENDPOINT
from ..transport import TcpEndpoint, recv_message, send_message, hex_from_bytes


class SocketDatabaseClient:
    """Database client that talks to `SocketDatabaseServer` over TCP."""

    def __init__(
        self,
        host: str = DEFAULT_DB_ENDPOINT.host,
        port: int = DEFAULT_DB_ENDPOINT.port,
        timeout_sec: float = 5.0,
        keepalive: bool = False,
    ):
        self.endpoint = TcpEndpoint(host, int(port))
        self.timeout_sec = float(timeout_sec)
        self.keepalive = bool(keepalive)
        self._socket = None

    def close(self) -> None:
        socket = self._socket
        self._socket = None
        if socket is not None:
            with suppress(Exception):
                send_message(socket, {"op": "Close"})
            with suppress(Exception):
                socket.close()

    def _get_socket(self):
        if not self.keepalive:
            return None
        if self._socket is None:
            self._socket = self.endpoint.connect(timeout_sec=self.timeout_sec)
        return self._socket

    def _request(self, payload: dict):
        sock = self._get_socket()
        if sock is None:
            with self.endpoint.connect(timeout_sec=self.timeout_sec) as tmp:
                send_message(tmp, payload)
                return recv_message(tmp)
        send_message(sock, payload)
        return recv_message(sock)

    def query(self, hash_bytes: bytes) -> Optional[Tuple[int, str]]:
        resp = self._request({"op": "Query", "hash": hex_from_bytes(hash_bytes)})
        if resp.get("status") != "ok":
            raise RuntimeError(resp.get("error") or "db error")
        return resp.get("result")

    def command(self, id_: int, new_name_str: str) -> bool:
        resp = self._request(
            {"op": "Command", "id": int(id_), "new_name": new_name_str}
        )
        if resp.get("status") != "ok":
            raise RuntimeError(resp.get("error") or "db error")
        return bool(resp.get("result"))
