import socket
from typing import Any, Dict

from ..metrics.collector import MetricsCollectorClient
from ..servers_config import DB_ENDPOINT
from ..transport_layer.tcp_server_base import (
    TcpServerBase,
    _exc_to_message,
)
from .engine import DbEngine
from .orchestrator import DbRequest, DbOrchestrator, LookupStrategy


class DbServer(TcpServerBase):
    """TCP wrapper around DbOrchestrator."""

    def __init__(
        self,
        lookup_strategy: LookupStrategy = DbEngine.STRATEGY_LINEAR,
        tcp_nodelay: bool = True,
    ) -> None:
        self.endpoint = DB_ENDPOINT
        self.tcp_nodelay = bool(tcp_nodelay)
        self._db_orchestrator = DbOrchestrator(lookup_strategy=lookup_strategy)
        self._metrics_client = MetricsCollectorClient()

        super().__init__(
            host=str(DB_ENDPOINT.host),
            port=int(DB_ENDPOINT.port),
            max_connections=128,
            listen_backlog=256,
            worker_pool_size=32,
            accept_timeout_sec=1.0,
            conn_timeout_sec=10.0,
            thread_name="socket-db",
            worker_thread_prefix="db-request",
        )

    def _on_connection_accepted(self, conn: socket.socket) -> None:
        if not self.tcp_nodelay:
            return
        try:
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError as exc:
            self._metrics_client.record_error(
                "db", f"failed to set TCP_NODELAY: {_exc_to_message(exc)}"
            )

    def _request_context(self, msg: Dict[str, Any]) -> str:
        return str(msg.get("op", "unknown"))

    def _on_close(self) -> None:
        self._metrics_client.close()
        self._db_orchestrator.close()

    def _record_error(self, message: str) -> None:
        self._metrics_client.record_error("db", message)

    def _record_metric(self, ok: bool, latency_ms: float) -> None:
        self._metrics_client.record("db", ok, latency_ms)

    def _parse_request(self, operation: str | None, req: Dict[str, Any]) -> DbRequest:
        if operation == "Query":
            hash_hex = req.get("hash")
            if not isinstance(hash_hex, str):
                raise ValueError("Query requires 'hash' hex string")
            return DbRequest("Query", {"hash": hash_hex})

        if operation == "Command":
            id_ = req.get("id")
            new_name = req.get("new_name")
            if not isinstance(id_, int) or not isinstance(new_name, str):
                raise ValueError("Command requires 'id' int and 'new_name' str")
            return DbRequest("Command", {"id": id_, "new_name": new_name})

        raise ValueError("unknown op")

    def _dispatch(self, req: Dict[str, Any]) -> Dict[str, Any]:
        op: str | None = req.get("op")
        request = self._parse_request(op, req)
        result = self._db_orchestrator.handle_request(request)
        return {"status": "ok", "result": result}

    def _handle_request_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        return self._dispatch(msg)
