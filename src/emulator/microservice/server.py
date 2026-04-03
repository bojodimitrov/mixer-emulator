from typing import Any, Dict

from ..metrics.collector import MetricsCollectorClient
from ..servers_config import SERVICE_ENDPOINT
from ..transport_layer.selector_json_server import (
    ConnState,
    SelectorJsonServer,
    _exc_to_message,
)
from .framework import Microservice


class MicroserviceServer(SelectorJsonServer):
    """TCP server that exposes the existing Microservice API."""

    def __init__(self, *, latency_ms: int = 15, pool_size: int = 128):
        self._service = Microservice(latency_ms=latency_ms, pool_size=pool_size)
        self._metrics_client = MetricsCollectorClient()
        self.max_requests_per_connection = 256

        super().__init__(
            host=str(SERVICE_ENDPOINT.host),
            port=int(SERVICE_ENDPOINT.port),
            max_connections=128,
            worker_pool_size=128,
            accept_timeout_sec=1.0,
            conn_timeout_sec=10.0,
            thread_name="socket-service",
            worker_thread_prefix="service-request",
        )

    def _request_context(self, msg: Dict[str, Any]) -> str:
        return f"method={msg.get('method')} path={msg.get('path')}"

    def _on_completion(self, state: ConnState, reply: Dict[str, Any]) -> None:
        state.handled += 1

    def _close_after_response(self, state: ConnState, reply: Dict[str, Any]) -> bool:
        return state.handled >= self.max_requests_per_connection

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
                "service request failed for "
                f"method={method} path={path}: {error_message}",
            )
            return {"status": "error", "error": error_message}

        result = {"status": "ok", "result": service_result}

        # Normalize tuple payloads to JSON-friendly lists.
        if result.get("status") == "ok" and isinstance(result.get("result"), tuple):
            rid, name = result["result"]
            result["result"] = [rid, name]

        return result
