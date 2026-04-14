from typing import Any, Dict, Optional

from emulator.servers_config import SERVICE_ENDPOINT
from emulator.transport_layer.tcp_client import TcpClient
from emulator.transport_layer.transport import TcpEndpoint


class MicroserviceClient(TcpClient):
    """Frontend client for `MicroserviceServer` over TCP."""

    def __init__(
        self,
        timeout_sec: float = 5.0,
        pool_size: int = 8,
        max_retries: int = 5,
        retry_backoff_ms: float = 5.0,
        max_idle_sec: Optional[float] = None,
        max_lifetime_sec: Optional[float] = None,
        eager_connect: bool = False,
    ):
        super().__init__(
            endpoint=TcpEndpoint(SERVICE_ENDPOINT.host, int(SERVICE_ENDPOINT.port)),
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
        return RuntimeError("microservice client is stopped")

    def _build_non_retryable_request_error(self, exc: Exception) -> Exception:
        return RuntimeError(
            "microservice request failed with non-retryable error"
        )

    def _build_retries_exhausted_error(self, exc: Exception) -> Exception:
        return RuntimeError("microservice request failed after retries exhausted")

    def request(
        self,
        method: str,
        data: Dict[str, Any],
        path: str,
    ) -> Dict[str, Any]:
        """Send a request to the microservice."""
        if self._stopped:
            raise RuntimeError("microservice client is stopped")

        payload = {
            "method": str(method).upper(),
            "path": path,
            "data": data,
        }
        return self._request(payload)
