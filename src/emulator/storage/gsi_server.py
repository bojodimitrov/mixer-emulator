"""Global Secondary Index (GSI) server.

Wraps a BPlusTreeIndex and exposes it over TCP so microservice instances can
perform hash → id lookups and trigger index updates after shard writes,
without touching the index file directly.

Protocol (JSON over the shared length-prefixed framing):

  Lookup:  {"op": "Lookup", "hash": "<hash_hex>"}
           → {"status": "ok", "result": <int|null>}

  Update:  {"op": "Update", "old_hash": "<hex>", "new_hash": "<hex>", "id": <int>}
           → {"status": "ok", "result": <bool>}
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional

from ..metrics.collector import MetricsCollectorClient
from ..servers_config import GSI_ENDPOINT
from ..transport_layer.tcp_server_base import TcpServerBase
from .b_tree_index import BPlusTreeIndex
from .constants import GSI_INDEX_PATH


class GsiServer(TcpServerBase):
    """TCP server that wraps a writable BPlusTreeIndex as the Global Secondary Index."""

    def __init__(
        self,
        index_path: Optional[str] = None,
        host: str = GSI_ENDPOINT.host,
        port: int = GSI_ENDPOINT.port,
    ) -> None:
        self._index_path = index_path or GSI_INDEX_PATH
        self._index: Optional[BPlusTreeIndex] = None
        self._metrics_client = MetricsCollectorClient()
        super().__init__(
            host=str(host),
            port=int(port),
            max_connections=256,
            listen_backlog=512,
            worker_pool_size=32,
            accept_timeout_sec=1.0,
            conn_timeout_sec=60.0,
            thread_name="socket-gsi",
            worker_thread_prefix="gsi-request",
        )

    def start(self) -> None:
        if not os.path.exists(self._index_path):
            raise FileNotFoundError(
                f"GSI index file not found: {self._index_path}. "
                "Build it first with `python -m emulator.storage.b_tree_index`."
            )
        self._index = BPlusTreeIndex(self._index_path, writable=True)
        super().start()

    def _on_close(self) -> None:
        self._metrics_client.close()
        if self._index is not None:
            self._index.close()
            self._index = None

    def _request_context(self, msg: Dict[str, Any]) -> str:
        return str(msg.get("op", "unknown"))

    def _handle_request_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        op = msg.get("op")

        if op == "Lookup":
            hash_hex = msg.get("hash")
            if not isinstance(hash_hex, str):
                raise ValueError("Lookup requires 'hash' hex string")
            assert self._index is not None
            result = self._index.query_by_hash(hash_hex)
            return {"status": "ok", "result": result}

        if op == "Update":
            old_hash_hex = msg.get("old_hash")
            new_hash_hex = msg.get("new_hash")
            id_ = msg.get("id")
            if (
                not isinstance(old_hash_hex, str)
                or not isinstance(new_hash_hex, str)
                or not isinstance(id_, int)
            ):
                raise ValueError("Update requires 'old_hash', 'new_hash' str and 'id' int")
            assert self._index is not None
            result = self._index.update(
                bytes.fromhex(old_hash_hex), bytes.fromhex(new_hash_hex), int(id_)
            )
            return {"status": "ok", "result": result}

        raise ValueError(f"unknown GSI op: {op!r}")

    def _record_error(self, message: str) -> None:
        self._metrics_client.record_error("gsi", message)

    def _record_metric(self, ok: bool, latency_ms: float) -> None:
        self._metrics_client.record("db", ok, latency_ms)
