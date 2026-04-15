"""Client for the Global Secondary Index (GSI) server."""
from __future__ import annotations

from contextlib import suppress
from typing import Optional

from ..servers_config import GSI_ENDPOINT
from ..transport_layer.tcp_client import TcpClient
from ..transport_layer.transport import TcpEndpoint, send_message


class GsiClient(TcpClient):
    """TCP client for GsiServer — hash → id lookup and index updates."""

    def __init__(
        self,
        host: str = GSI_ENDPOINT.host,
        port: int = GSI_ENDPOINT.port,
        timeout_sec: float = 10.0,
        pool_size: int = 8,
        max_idle_sec: Optional[float] = 45.0,
        max_retries: int = 3,
        retry_backoff_ms: float = 50.0,
    ) -> None:
        super().__init__(
            endpoint=TcpEndpoint(str(host), int(port)),
            timeout_sec=timeout_sec,
            pool_size=pool_size,
            eager_connect=False,
            max_idle_sec=max_idle_sec,
            max_retries=max_retries,
            retry_backoff_ms=retry_backoff_ms,
            retry_unpooled=False,
        )

    def _build_closed_error(self) -> RuntimeError:
        return RuntimeError("gsi client is closed")

    def _close_socket(self, sock) -> None:
        with suppress(Exception):
            send_message(sock, {"op": "Close"})
        super()._close_socket(sock)

    def lookup(self, hash_hex: str) -> Optional[int]:
        """Return the global record id for hash_hex, or None if not in the index."""
        resp = self._request({"op": "Lookup", "hash": hash_hex})
        if resp.get("status") != "ok":
            raise RuntimeError(resp.get("error") or "gsi lookup error")
        return resp.get("result")

    def update(self, old_hash_hex: str, new_hash_hex: str, id_: int) -> bool:
        """Update the index entry: remove old_hash, insert new_hash → id."""
        resp = self._request({
            "op": "Update",
            "old_hash": old_hash_hex,
            "new_hash": new_hash_hex,
            "id": int(id_),
        })
        if resp.get("status") != "ok":
            raise RuntimeError(resp.get("error") or "gsi update error")
        return bool(resp.get("result"))
