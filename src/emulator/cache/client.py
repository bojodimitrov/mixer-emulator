from __future__ import annotations

from typing import Any, Dict, Optional

from ..servers_config import CACHE_ENDPOINT
from ..transport_layer.tcp_client import TcpClient
from ..transport_layer.transport import TcpEndpoint


class CacheClient(TcpClient):
    """TCP client for the distributed cache server."""

    def __init__(
        self,
        timeout_sec: float = 1.0,
        pool_size: int = 4,
        eager_connect: bool = False,
        max_idle_sec: Optional[float] = None,
        max_lifetime_sec: Optional[float] = None,
        max_retries: int = 2,
        retry_backoff_ms: float = 5.0,
    ) -> None:
        super().__init__(
            endpoint=TcpEndpoint(CACHE_ENDPOINT.host, int(CACHE_ENDPOINT.port)),
            timeout_sec=timeout_sec,
            pool_size=pool_size,
            eager_connect=eager_connect,
            max_idle_sec=max_idle_sec,
            max_lifetime_sec=max_lifetime_sec,
            max_retries=max_retries,
            retry_backoff_ms=retry_backoff_ms,
            retry_unpooled=True,
        )

    def _build_closed_error(self) -> RuntimeError:
        return RuntimeError("cache client is stopped")

    def _build_non_retryable_request_error(self, exc: Exception) -> Exception:
        return RuntimeError("cache request failed with non-retryable error")

    def _build_retries_exhausted_error(self, exc: Exception) -> Exception:
        return RuntimeError("cache request failed after retries exhausted")

    def _send(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = self._request(payload)
        if response.get("status") != "ok":
            raise RuntimeError(str(response.get("error") or "cache error"))
        return response

    def ping(self) -> bool:
        return bool(self._send({"op": "Ping"}).get("result"))

    def get(self, key: str) -> Any:
        return self._send({"op": "Get", "key": str(key)}).get("result")

    def exists(self, key: str) -> bool:
        return bool(self._send({"op": "Exists", "key": str(key)}).get("result"))

    def mget(self, keys: list[str]) -> list[Any]:
        payload = {"op": "MGet", "keys": [str(key) for key in keys]}
        result = self._send(payload).get("result")
        return list(result) if isinstance(result, list) else []

    def incr(self, key: str, amount: int = 1) -> int:
        payload = {"op": "Incr", "key": str(key), "amount": int(amount)}
        result = self._send(payload).get("result")
        if not isinstance(result, int):
            raise RuntimeError("cache INCR returned a non-integer result")
        return result

    def set(self, key: str, value: Any, *, ttl_sec: Optional[float] = None) -> bool:
        payload: Dict[str, Any] = {"op": "Set", "key": str(key), "value": value}
        if ttl_sec is not None:
            payload["ttl_sec"] = float(ttl_sec)
        return bool(self._send(payload).get("result"))

    def delete(self, key: str) -> bool:
        return bool(self._send({"op": "Delete", "key": str(key)}).get("result"))

    def flush(self) -> bool:
        return bool(self._send({"op": "Flush"}).get("result"))