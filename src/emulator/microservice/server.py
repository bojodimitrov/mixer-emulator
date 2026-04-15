from dataclasses import dataclass
from typing import Any, Dict

from ..metrics.collector import MetricsCollectorClient
from ..transport_layer.tcp_server_base import (
    ConnState,
    TcpServerBase,
    _exc_to_message,
)
from .framework import Microservice


@dataclass
class _MicroserviceConnState(ConnState):
    handled: int = 0


class MicroserviceServer(TcpServerBase):
    """TCP server that exposes the existing Microservice API."""

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 50100,
        latency_ms: int = 15,
        pool_size: int = 128,
        max_connections: int = 480,
        listen_backlog: int = 1024,
    ):
        self._service = Microservice(latency_ms=latency_ms, pool_size=pool_size)
        self._metrics_client = MetricsCollectorClient()
        self.max_requests_per_connection = 4096
        self.total_handled = 0

        super().__init__(
            host=str(host),
            port=int(port),
            max_connections=max_connections,
            listen_backlog=listen_backlog,
            worker_pool_size=128,
            accept_timeout_sec=1.0,
            conn_timeout_sec=10.0,
            thread_name="socket-service",
            worker_thread_prefix="service-request",
        )

    def _request_context(self, msg: Dict[str, Any]) -> str:
        return f"method={msg.get('method')} path={msg.get('path')}"

    def _make_conn_state(self) -> ConnState:
        return _MicroserviceConnState()

    def _on_completion(self, state: ConnState, reply: Dict[str, Any]) -> None:
        if isinstance(state, _MicroserviceConnState):
            state.handled += 1
            self.total_handled += 1

    def _close_after_response(self, state: ConnState, reply: Dict[str, Any]) -> bool:
        if isinstance(state, _MicroserviceConnState):
            return state.handled >= self.max_requests_per_connection
        return False

    def _on_close(self) -> None:
        self._metrics_client.close()
        self._service.stop()

    def _record_error(self, message: str) -> None:
        self._metrics_client.record_error("service", message)

    def _record_metric(self, ok: bool, latency_ms: float) -> None:
        self._metrics_client.record("service", ok, latency_ms)

    def _handle_request_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        method = (msg.get("method") or "").upper()
        path = msg.get("path") or "/"
        data = msg.get("data")

        if not isinstance(data, dict):
            raise ValueError("request 'data' must be an object")

        try:
            service_result = self._service.handle(method, path, data)
        except Exception as exc:
            error_message = _exc_to_message(exc)
            self._metrics_client.record_error(
                "service",
                f"{method} {path} failed: {error_message}",
            )
            return {"status": "error", "error": error_message}

        result = {"status": "ok", "result": service_result}

        # Normalize tuple payloads to JSON-friendly lists.
        if result.get("status") == "ok" and isinstance(result.get("result"), tuple):
            rid, name = result["result"]
            result["result"] = [rid, name]

        return result
