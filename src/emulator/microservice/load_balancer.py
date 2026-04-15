"""Round-robin TCP load balancer for microservice instances.

The load balancer is intentionally decoupled from the microservice lifecycle:
it only holds a list of backend addresses and proxies requests to them.
The caller is responsible for starting/stopping the backend services.
"""

import itertools
import threading
from typing import Any, Dict, List

from ..servers_config import LOAD_BALANCER_ENDPOINT
from ..transport_layer.tcp_client import TcpClient
from ..transport_layer.tcp_server_base import ConnState, TcpServerBase, _exc_to_message
from ..transport_layer.transport import TcpEndpoint


class LoadBalancerServer(TcpServerBase):
    """TCP server that distributes incoming requests across backend endpoints.

    Parameters
    ----------
    backends:
        List of ``TcpEndpoint`` values pointing at the microservice instances.
        The load balancer never starts, stops, or monitors these services —
        it only forwards requests to them in round-robin order.
    host / port:
        Address for the load balancer to listen on.
        Defaults to ``LOAD_BALANCER_ENDPOINT`` (127.0.0.1:50005).
    pool_size_per_backend:
        Number of pooled connections kept open to each backend.
    max_connections:
        Maximum simultaneous client connections accepted by the LB.
    """

    def __init__(
        self,
        backends: List[TcpEndpoint],
        *,
        host: str = LOAD_BALANCER_ENDPOINT.host,
        port: int = LOAD_BALANCER_ENDPOINT.port,
        pool_size_per_backend: int = 64,
        max_connections: int = 480,
        listen_backlog: int = 1024,
    ) -> None:
        if not backends:
            raise ValueError("at least one backend endpoint is required")

        # One TcpClient (with its own connection pool) per backend address.
        # The LB owns these clients and closes them on shutdown.
        # max_lifetime_sec is set just below the server-side max_requests_per_connection
        # threshold so pools retire sockets proactively before the server closes them,
        # preventing ECONNRESET races under high traffic.
        self._backend_clients: List[TcpClient] = [
            TcpClient(
                endpoint=ep,
                timeout_sec=5.0,
                pool_size=pool_size_per_backend,
                max_lifetime_sec=120.0,
                max_retries=2,
                retry_backoff_ms=5.0,
                retry_unpooled=True,
            )
            for ep in backends
        ]

        # Thread-safe round-robin counter.
        self._rr_cycle = itertools.cycle(range(len(self._backend_clients)))
        self._rr_lock = threading.Lock()

        super().__init__(
            host=host,
            port=port,
            max_connections=max_connections,
            listen_backlog=listen_backlog,
            worker_pool_size=128,
            accept_timeout_sec=1.0,
            conn_timeout_sec=30.0,
            thread_name="socket-load-balancer",
            worker_thread_prefix="lb-request",
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _pick_backend(self) -> TcpClient:
        """Return the next backend client in round-robin order."""
        with self._rr_lock:
            idx = next(self._rr_cycle)
        return self._backend_clients[idx]

    # ------------------------------------------------------------------
    # TcpServerBase abstract / hook implementations
    # ------------------------------------------------------------------

    def _handle_request_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """Forward the raw request dict to a backend and return its response."""
        backend = self._pick_backend()
        return backend._request(msg)

    def _record_error(self, message: str) -> None:
        # Extend this to wire in a MetricsCollectorClient if desired.
        pass

    def _record_metric(self, ok: bool, latency_ms: float) -> None:
        pass

    def _on_close(self) -> None:
        """Close all pooled backend connections on shutdown."""
        for client in self._backend_clients:
            client.close()
