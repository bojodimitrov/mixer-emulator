from __future__ import annotations

from contextlib import suppress
from typing import Optional, Tuple

from ..servers_config import DB_ENDPOINT
from ..transport_layer.tcp_client import TcpClient
from ..transport_layer.transport import TcpEndpoint, send_message


class DbClient(TcpClient):
    """Database client that talks to `DbServer` over TCP."""

    def __init__(
        self,
        timeout_sec: float = 10.0,
        pool_size: int = 0,
        eager_connect: bool = True,
        max_idle_sec: Optional[float] = None,
        max_lifetime_sec: Optional[float] = None,
        max_retries: int = 1,
        retry_backoff_ms: float = 0.0,
    ) -> None:
        super().__init__(
            endpoint=TcpEndpoint(DB_ENDPOINT.host, int(DB_ENDPOINT.port)),
            timeout_sec=timeout_sec,
            pool_size=pool_size,
            eager_connect=eager_connect,
            max_idle_sec=max_idle_sec,
            max_lifetime_sec=max_lifetime_sec,
            max_retries=max_retries,
            retry_backoff_ms=retry_backoff_ms,
            retry_unpooled=False,
        )

    def _build_closed_error(self) -> RuntimeError:
        return RuntimeError("db client is closed")

    def _close_socket(self, sock) -> None:
        with suppress(Exception):
            send_message(sock, {"op": "Close"})
        super()._close_socket(sock)

    def query(self, hash: str) -> Optional[Tuple[int, str]]:
        resp = self._request({"op": "Query", "hash": hash})
        if resp.get("status") != "ok":
            raise RuntimeError(resp.get("error") or "db error")
        return resp.get("result")

    def command(self, id_: int, new_name_str: str) -> int:
        resp = self._request(
            {"op": "Command", "id": int(id_), "new_name": new_name_str}
        )
        if resp.get("status") != "ok":
            raise RuntimeError(resp.get("error") or "db error")
        result = resp.get("result")
        if isinstance(result, bool) or not isinstance(result, int):
            raise RuntimeError("db command payload was not an integer")
        return result
