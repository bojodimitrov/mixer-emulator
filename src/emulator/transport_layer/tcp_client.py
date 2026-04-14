from __future__ import annotations

import queue
import socket
import threading
import time
from contextlib import suppress
from typing import Any, Dict, Optional

from .transport import TcpEndpoint, recv_message, send_message


class TcpClient:
    """Shared TCP client with optional socket pooling and retries."""

    def __init__(
        self,
        *,
        endpoint: TcpEndpoint,
        timeout_sec: float,
        pool_size: int = 0,
        eager_connect: bool = False,
        max_idle_sec: Optional[float] = None,
        max_lifetime_sec: Optional[float] = None,
        max_retries: int = 0,
        retry_backoff_ms: float = 0.0,
        retry_unpooled: bool = False,
    ) -> None:
        self.endpoint = endpoint
        self.timeout_sec = float(timeout_sec)
        if pool_size < 0:
            raise ValueError("pool_size must be >= 0")
        self.pool_size = int(pool_size)
        self.eager_connect = bool(eager_connect)
        if max_idle_sec is not None and float(max_idle_sec) < 0:
            raise ValueError("max_idle_sec must be >= 0 or None")
        if max_lifetime_sec is not None and float(max_lifetime_sec) < 0:
            raise ValueError("max_lifetime_sec must be >= 0 or None")
        self.max_idle_sec = None if max_idle_sec is None else float(max_idle_sec)
        self.max_lifetime_sec = (
            None if max_lifetime_sec is None else float(max_lifetime_sec)
        )
        self.max_retries = max(0, int(max_retries))
        self.retry_backoff_ms = max(0.0, float(retry_backoff_ms))
        self._retry_unpooled = bool(retry_unpooled)

        self._pool: Optional[queue.LifoQueue[socket.socket]] = None
        self._pool_lock = threading.Lock()
        self._created = 0
        self._stopped = False
        self._socket_timestamps: dict[int, tuple[float, float]] = {}

        if self.pool_size > 0:
            self._pool = queue.LifoQueue(maxsize=self.pool_size)

            if self.eager_connect:
                eager = max(0, self.pool_size // 2)
                for _ in range(eager):
                    sock = self._new_socket()
                    self._created += 1
                    self._pool.put_nowait(sock)

    def __del__(self) -> None:
        self.close()

    def _build_closed_error(self) -> RuntimeError:
        return RuntimeError("tcp client is closed")

    def _build_non_retryable_request_error(self, exc: Exception) -> Exception:
        return exc

    def _build_retries_exhausted_error(self, exc: Exception) -> Exception:
        return exc

    def _new_socket(self) -> socket.socket:
        sock = self.endpoint.connect(timeout_sec=self.timeout_sec)
        with suppress(Exception):
            sock.settimeout(self.timeout_sec)
        now = time.monotonic()
        self._socket_timestamps[id(sock)] = (now, now)
        return sock

    def _close_socket(self, sock: socket.socket) -> None:
        with suppress(Exception):
            sock.close()

    def close(self) -> None:
        self._stopped = True
        pool = self._pool
        self._pool = None
        if pool is not None:
            while True:
                try:
                    sock = pool.get_nowait()
                except queue.Empty:
                    break
                self._close_socket(sock)
                self._socket_timestamps.pop(id(sock), None)

        with self._pool_lock:
            self._created = 0
            self._socket_timestamps.clear()

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
        self._close_socket(sock)
        self._socket_timestamps.pop(id(sock), None)
        with self._pool_lock:
            self._created = max(0, self._created - 1)

    def _pool_acquire(self) -> socket.socket:
        pool = self._pool
        if pool is None:
            raise RuntimeError("pool not enabled")

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
            self._close_socket(sock)
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

    def _is_retryable_transport_error(self, exc: Exception) -> bool:
        return isinstance(exc, (OSError, ConnectionError, TimeoutError, socket.timeout))

    def _roundtrip(self, sock: socket.socket, payload: Dict[str, Any]) -> Dict[str, Any]:
        send_message(sock, payload)
        return recv_message(sock)

    def _raise_mapped_exception(
        self,
        builder: Any,
        exc: Exception,
    ) -> None:
        mapped = builder(exc)
        if mapped is exc:
            raise exc
        raise mapped from exc

    def _request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if self._stopped:
            raise self._build_closed_error()

        retry_enabled = self._pool is not None or self._retry_unpooled
        attempts = self.max_retries + 1 if retry_enabled else 1
        last_exc: Optional[Exception] = None

        for attempt in range(attempts):
            sock: Optional[socket.socket] = None
            try:
                if self._pool is None:
                    with self._new_socket() as tmp:
                        return self._roundtrip(tmp, payload)

                sock = self._pool_acquire()
                response = self._roundtrip(sock, payload)
                if sock.fileno() != -1:
                    self._pool_release(sock)
                    sock = None
                return response
            except Exception as exc:
                last_exc = exc
                if sock is not None:
                    self._pool_drop(sock)

                retryable = self._is_retryable_transport_error(exc)
                if retryable and attempt < attempts - 1:
                    if self.retry_backoff_ms > 0:
                        time.sleep(self.retry_backoff_ms / 1000.0)
                    continue
                if retryable and attempts > 1:
                    self._raise_mapped_exception(
                        self._build_retries_exhausted_error,
                        exc,
                    )
                self._raise_mapped_exception(
                    self._build_non_retryable_request_error,
                    exc,
                )

        if last_exc is not None:
            self._raise_mapped_exception(self._build_retries_exhausted_error, last_exc)
        raise RuntimeError("tcp request failed")