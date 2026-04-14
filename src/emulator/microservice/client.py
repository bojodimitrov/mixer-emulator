import queue
import os
import socket
import threading
import time
from contextlib import suppress
from typing import Any, Dict, Optional

from emulator.servers_config import SERVICE_ENDPOINT
from emulator.transport_layer.transport import recv_message, send_message


class MicroserviceClient:
    """Frontend client for `MicroserviceServer` over TCP."""

    _global_gate_lock = threading.Lock()
    _global_request_gates: dict[str, threading.Semaphore] = {}
    _global_request_gate_sizes: dict[str, int] = {}

    def __init__(
        self,
        timeout_sec: float = 5.0,
        pool_size: int = 8,
        max_retries: int = 5,
        retry_backoff_ms: float = 5.0,
        max_idle_sec: Optional[float] = None,
        max_lifetime_sec: Optional[float] = None,
        eager_connect: bool = False,
        max_inflight_global: Optional[int] = None,
        inflight_group: str = "default",
    ):
        from ..transport_layer.transport import TcpEndpoint

        self.endpoint = TcpEndpoint(SERVICE_ENDPOINT.host, int(SERVICE_ENDPOINT.port))
        self.timeout_sec = float(timeout_sec)
        if pool_size < 0:
            raise ValueError("pool_size must be >= 0")
        self.pool_size = int(pool_size)
        self.max_retries = max(0, int(max_retries))
        self.retry_backoff_ms = max(0.0, float(retry_backoff_ms))
        if max_idle_sec is not None and float(max_idle_sec) < 0:
            raise ValueError("max_idle_sec must be >= 0 or None")
        if max_lifetime_sec is not None and float(max_lifetime_sec) < 0:
            raise ValueError("max_lifetime_sec must be >= 0 or None")
        self.max_idle_sec = None if max_idle_sec is None else float(max_idle_sec)
        self.max_lifetime_sec = (
            None if max_lifetime_sec is None else float(max_lifetime_sec)
        )
        self.eager_connect = bool(eager_connect)

        if max_inflight_global is None:
            # Windows uses select()-based selectors with descriptor limits.
            # Keep a shared frontend request gate below service FD headroom.
            inferred_max_inflight_global: Optional[int] = (
                384 if os.name == "nt" else None
            )
        else:
            inferred_max_inflight_global = int(max_inflight_global)
            if inferred_max_inflight_global <= 0:
                raise ValueError("max_inflight_global must be > 0 or None")

        self.max_inflight_global = inferred_max_inflight_global
        self.inflight_group = str(inflight_group or "default")
        self._global_request_semaphore = self._get_global_request_semaphore(
            self.inflight_group, self.max_inflight_global
        )

        self._pool: Optional[queue.LifoQueue[socket.socket]] = None
        self._pool_lock = threading.Lock()
        self._created = 0
        self._stopped = False
        self._socket_timestamps: dict[int, tuple[float, float]] = {}

        if self.pool_size > 0:
            self._pool = queue.LifoQueue(maxsize=self.pool_size)

            # Eagerly create half the pool to reduce first-request connect latency.
            if self.eager_connect:
                eager = max(0, self.pool_size // 2)
                for _ in range(eager):
                    s = self._new_socket()
                    self._created += 1
                    self._pool.put_nowait(s)

    @classmethod
    def _get_global_request_semaphore(
        cls, group: str, size: Optional[int]
    ) -> Optional[threading.Semaphore]:
        if size is None:
            return None
        with cls._global_gate_lock:
            gate = cls._global_request_gates.get(group)
            gate_size = cls._global_request_gate_sizes.get(group)
            if gate is None or gate_size != size:
                gate = threading.Semaphore(size)
                cls._global_request_gates[group] = gate
                cls._global_request_gate_sizes[group] = size
            return gate

    def _new_socket(self) -> socket.socket:
        sock = self.endpoint.connect(timeout_sec=self.timeout_sec)
        sock.settimeout(self.timeout_sec)
        now = time.monotonic()
        self._socket_timestamps[id(sock)] = (now, now)
        return sock

    def _mark_socket_released(self, sock: socket.socket) -> None:
        key = id(sock)
        now = time.monotonic()
        created_last = self._socket_timestamps.get(key)
        if created_last is None:
            self._socket_timestamps[key] = (now, now)
            return
        created_at, _ = created_last
        self._socket_timestamps[key] = (created_at, now)

    def _is_socket_expired(self, sock: socket.socket, now: float) -> bool:
        if sock.fileno() == -1:
            return True

        created_last = self._socket_timestamps.get(id(sock))
        if created_last is None:
            return False
        created_at, last_used_at = created_last

        if (
            self.max_lifetime_sec is not None
            and now - created_at > self.max_lifetime_sec
        ):
            return True
        if self.max_idle_sec is not None and now - last_used_at > self.max_idle_sec:
            return True
        return False

    def _discard_pooled_socket(self, sock: socket.socket) -> None:
        with suppress(Exception):
            sock.close()
        self._socket_timestamps.pop(id(sock), None)
        with self._pool_lock:
            self._created = max(0, self._created - 1)

    def _pool_acquire(self) -> socket.socket:
        pool = self._pool
        if pool is None:
            return self._new_socket()

        while True:
            try:
                sock = pool.get_nowait()
            except queue.Empty:
                break
            if not self._is_socket_expired(sock, time.monotonic()):
                return sock
            self._discard_pooled_socket(sock)

        with self._pool_lock:
            if self._created < self.pool_size:
                sock = self._new_socket()
                self._created += 1
                return sock

        while True:
            sock = pool.get()
            if not self._is_socket_expired(sock, time.monotonic()):
                return sock
            self._discard_pooled_socket(sock)

            with self._pool_lock:
                if self._created < self.pool_size:
                    sock = self._new_socket()
                    self._created += 1
                    return sock

    def _pool_release(self, sock: socket.socket) -> None:
        pool = self._pool
        if pool is None:
            with suppress(Exception):
                sock.close()
            self._socket_timestamps.pop(id(sock), None)
            return

        self._mark_socket_released(sock)
        if self._is_socket_expired(sock, time.monotonic()):
            self._discard_pooled_socket(sock)
            return

        try:
            pool.put_nowait(sock)
        except queue.Full:
            self._discard_pooled_socket(sock)

    def _pool_drop(self, sock: socket.socket) -> None:
        self._discard_pooled_socket(sock)

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
            self._socket_timestamps.pop(id(sock), None)

        with self._pool_lock:
            self._created = 0
            self._socket_timestamps.clear()

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

        gate = self._global_request_semaphore
        acquired_gate = False
        if gate is not None:
            gate.acquire()
            acquired_gate = True

        try:
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
                        raise RuntimeError(
                            "microservice request failed with non-retryable error"
                        ) from exc

                    if attempt >= self.max_retries:
                        raise RuntimeError(
                            "microservice request failed after retries exhausted"
                        ) from exc

                    if self.retry_backoff_ms > 0:
                        time.sleep(self.retry_backoff_ms / 1000.0)

            raise RuntimeError("microservice request failed") from last_exc
        finally:
            if gate is not None and acquired_gate:
                gate.release()
