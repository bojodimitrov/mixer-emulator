from __future__ import annotations

from typing import Any, Dict, Optional

from ..metrics.collector import MetricsCollectorClient
from ..servers_config import CACHE_ENDPOINT
from ..transport_layer.tcp_server_base import TcpServerBase
from .store import CacheStore


class CacheServer(TcpServerBase):
    """Small Redis-like TCP cache server for key/value operations."""

    def __init__(
        self,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        max_connections: int = 256,
        listen_backlog: int = 512,
        worker_pool_size: int = 32,
    ) -> None:
        self._store = CacheStore()
        self._metrics_client = MetricsCollectorClient()
        super().__init__(
            host=str(host or CACHE_ENDPOINT.host),
            port=int(port or CACHE_ENDPOINT.port),
            max_connections=max_connections,
            listen_backlog=listen_backlog,
            worker_pool_size=worker_pool_size,
            accept_timeout_sec=1.0,
            conn_timeout_sec=10.0,
            thread_name="socket-cache",
            worker_thread_prefix="cache-request",
        )

    def _request_context(self, msg: Dict[str, Any]) -> str:
        return str(msg.get("op", "unknown"))

    def _on_close(self) -> None:
        self._metrics_client.close()
        self._store.clear()

    def _record_error(self, message: str) -> None:
        self._metrics_client.record_error("cache", message)

    def _record_metric(self, ok: bool, latency_ms: float) -> None:
        return

    def _require_key(self, msg: Dict[str, Any]) -> str:
        key = msg.get("key")
        if not isinstance(key, str) or not key:
            raise ValueError("request requires non-empty 'key' string")
        return key

    def _require_keys(self, msg: Dict[str, Any]) -> list[str]:
        keys = msg.get("keys")
        if not isinstance(keys, list) or not keys:
            raise ValueError("request requires non-empty 'keys' list")
        if not all(isinstance(key, str) and key for key in keys):
            raise ValueError("all keys must be non-empty strings")
        return list(keys)

    def _handle_request_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        op = msg.get("op")
        if op == "Ping":
            return {"status": "ok", "result": True}

        if op == "Get":
            return {"status": "ok", "result": self._store.get(self._require_key(msg))}

        if op == "Exists":
            return {
                "status": "ok",
                "result": self._store.exists(self._require_key(msg)),
            }

        if op == "MGet":
            return {"status": "ok", "result": self._store.mget(self._require_keys(msg))}

        if op == "Set":
            key = self._require_key(msg)
            if "value" not in msg:
                raise ValueError("Set requires 'value'")
            ttl_sec = msg.get("ttl_sec")
            if ttl_sec is not None and not isinstance(ttl_sec, (int, float)):
                raise ValueError("ttl_sec must be numeric or omitted")
            return {
                "status": "ok",
                "result": self._store.set(key, msg.get("value"), ttl_sec=ttl_sec),
            }

        if op == "Incr":
            key = self._require_key(msg)
            amount = msg.get("amount", 1)
            if not isinstance(amount, int):
                raise ValueError("amount must be an integer")
            return {"status": "ok", "result": self._store.incr(key, amount=amount)}

        if op == "Delete":
            return {
                "status": "ok",
                "result": self._store.delete(self._require_key(msg)),
            }

        if op == "Flush":
            self._store.clear()
            return {"status": "ok", "result": True}

        raise ValueError("unknown cache op")