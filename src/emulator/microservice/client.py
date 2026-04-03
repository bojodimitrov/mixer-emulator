import queue
import socket
import threading
import time
from contextlib import suppress
from typing import Any, Dict, Optional

from emulator.servers_config import SERVICE_ENDPOINT
from emulator.transport_layer.transport import recv_message, send_message


class MicroserviceClient:
    """Frontend client for `MicroserviceServer` over TCP."""

    def __init__(
        self,
        timeout_sec: float = 10.0,
        pool_size: int = 8,
        max_retries: int = 20,
        retry_backoff_ms: float = 5.0,
    ):
        from ..transport_layer.transport import TcpEndpoint

        self.endpoint = TcpEndpoint(SERVICE_ENDPOINT.host, int(SERVICE_ENDPOINT.port))
        self.timeout_sec = float(timeout_sec)
        if pool_size < 0:
            raise ValueError("pool_size must be >= 0")
        self.pool_size = int(pool_size)
        self.max_retries = max(0, int(max_retries))
        self.retry_backoff_ms = max(0.0, float(retry_backoff_ms))

        self._pool: Optional[queue.LifoQueue[socket.socket]] = None
        self._pool_lock = threading.Lock()
        self._created = 0
        self._stopped = False

        if self.pool_size > 0:
            self._pool = queue.LifoQueue(maxsize=self.pool_size)

    def _new_socket(self) -> socket.socket:
        sock = self.endpoint.connect(timeout_sec=self.timeout_sec)
        sock.settimeout(self.timeout_sec)
        return sock

    def _pool_acquire(self) -> socket.socket:
        pool = self._pool
        if pool is None:
            return self._new_socket()

        try:
            return pool.get_nowait()
        except queue.Empty:
            pass

        with self._pool_lock:
            if self._created < self.pool_size:
                sock = self._new_socket()
                self._created += 1
                return sock

        return pool.get(timeout=self.timeout_sec)

    def _pool_release(self, sock: socket.socket) -> None:
        pool = self._pool
        if pool is None:
            with suppress(Exception):
                sock.close()
            return

        try:
            pool.put_nowait(sock)
        except queue.Full:
            with suppress(Exception):
                sock.close()

    def _pool_drop(self, sock: socket.socket) -> None:
        with suppress(Exception):
            sock.close()

        if self._pool is not None:
            with self._pool_lock:
                self._created = max(0, self._created - 1)

    def _is_retryable(self, exc: Exception) -> bool:
        return isinstance(exc, (OSError, ConnectionError, TimeoutError, socket.timeout))

    def close(self) -> None:
        self._stopped = True
        pool = self._pool
        self._pool = None
        if pool is None:
            return

        while True:
            try:
                sock = pool.get_nowait()
            except queue.Empty:
                break
            with suppress(Exception):
                sock.close()

    def __del__(self) -> None:
        # Best-effort cleanup for call sites that do not call close().
        self.close()

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

        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            sock: Optional[socket.socket] = None
            try:
                sock = self._pool_acquire()
                send_message(sock, payload)
                response = recv_message(sock)
                if sock.fileno() != -1:
                    self._pool_release(sock)
                return response
            except Exception as exc:
                last_exc = exc
                if sock is not None:
                    self._pool_drop(sock)

                if not self._is_retryable(exc):
                    self.close()
                    raise RuntimeError(
                        "microservice client stopped after non-retryable error"
                    ) from exc

                if attempt >= self.max_retries:
                    self.close()
                    raise RuntimeError(
                        "microservice client stopped after retries exhausted"
                    ) from exc

                if self.retry_backoff_ms > 0:
                    time.sleep(self.retry_backoff_ms / 1000.0)

        self.close()
        raise RuntimeError("microservice client stopped") from last_exc
