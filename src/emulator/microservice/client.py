from typing import Any, Dict

from emulator.servers_config import SERVICE_ENDPOINT
from emulator.transport_layer.transport import recv_message, send_message


class MicroserviceClient:
    """Frontend client for `MicroserviceServer` over TCP."""

    def __init__(
        self,
        timeout_sec: float = 10.0,
    ):
        from ..transport_layer.transport import TcpEndpoint

        self.endpoint = TcpEndpoint(SERVICE_ENDPOINT.host, int(SERVICE_ENDPOINT.port))
        self.timeout_sec = float(timeout_sec)

    def request(
        self,
        method: str,
        data: Dict[str, Any],
        path: str,
    ) -> Dict[str, Any]:
        """Send a request to the microservice."""
        with self.endpoint.connect(timeout_sec=self.timeout_sec) as sock:
            send_message(
                sock,
                {
                    "method": str(method).upper(),
                    "path": path,
                    "data": data,
                },
            )
            return recv_message(sock)
