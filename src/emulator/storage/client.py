import queue
import socket
import threading
import time
from contextlib import suppress
from typing import Optional, Tuple

from ..servers_config import DB_ENDPOINT
from ..transport_layer.transport import (
    TcpEndpoint,
    recv_message,
    send_message,
)


class DbClient:
    """Database client that talks to `DbServer` over TCP."""

    def __init__(
        self,
        timeout_sec: float = 10.0,
        pool_size: int = 0,
        eager_connect: bool = True,
        max_idle_sec: Optional[float] = None,
        max_lifetime_sec: Optional[float] = None,
        max_retries: int = 1,
        retry_backoff_ms: float = 0.0,
    ):
        self.endpoint = TcpEndpoint(DB_ENDPOINT.host, int(DB_ENDPOINT.port))
        self.timeout_sec = float(timeout_sec)

        # Connection management modes:
        # - default (pool_size==0): open a new TCP connection per request
        # - pool_size>0: a pool of reusable TCP connections (thread-safe)
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

        self._pool: Optional[queue.LifoQueue[socket.socket]] = None
        self._pool_lock = threading.Lock()
        self._created = 0
        self._stopped = False
        self._socket_timestamps: dict[int, tuple[float, float]] = {}
        if self.pool_size > 0:
            self._pool = queue.LifoQueue(maxsize=self.pool_size)

            # Eagerly create half the pool to reduce first-request connect overhead,
            # without paying the cost of creating the full pool up-front.
            if self.eager_connect:
                eager = max(0, self.pool_size // 2)
                for _ in range(eager):
                    s = self.endpoint.connect(timeout_sec=self.timeout_sec)
                    self._created += 1
                    now = time.monotonic()
                    self._socket_timestamps[id(s)] = (now, now)
                    self._pool.put_nowait(s)

    def __del__(self) -> None:
        # Best-effort cleanup for call sites that do not call close().
        self.close()

    def close(self) -> None:
        self._stopped = True
        pool = self._pool
        self._pool = None
        if pool is not None:
            while True:
                try:
                    s = pool.get_nowait()
                except queue.Empty:
                    break
                with suppress(Exception):
                    send_message(s, {"op": "Close"})
                with suppress(Exception):
                    s.close()
                self._socket_timestamps.pop(id(s), None)
        with self._pool_lock:
            self._created = 0
            self._socket_timestamps.clear()

    def _track_new_socket(self, sock: socket.socket) -> None:
        now = time.monotonic()
        self._socket_timestamps[id(sock)] = (now, now)

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
            raise RuntimeError("pool not enabled")

        # Drop obviously stale sockets before reuse.
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
                sock = self.endpoint.connect(timeout_sec=self.timeout_sec)
                self._created += 1
                self._track_new_socket(sock)
                return sock

        # Pool is at capacity; wait for a valid socket to be returned.
        while True:
            sock = pool.get()
            if not self._is_socket_expired(sock, time.monotonic()):
                return sock
            self._discard_pooled_socket(sock)

            with self._pool_lock:
                if self._created < self.pool_size:
                    sock = self.endpoint.connect(timeout_sec=self.timeout_sec)
                    self._created += 1
                    self._track_new_socket(sock)
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

    def _is_retryable_transport_error(self, exc: Exception) -> bool:
        return isinstance(exc, (OSError, ConnectionError, TimeoutError, socket.timeout))

    def _request(self, payload: dict):
        if self._stopped:
            raise RuntimeError("db client is closed")

        # Pool mode (thread-safe reuse, keep-alive protocol per socket).
        if self._pool is not None:
            last_exc: Optional[Exception] = None
            for attempt in range(self.max_retries + 1):
                sock = self._pool_acquire()
                try:
                    send_message(sock, payload)
                    return recv_message(sock)
                except Exception as exc:
                    last_exc = exc
                    # Discard the broken socket and update the live-count.
                    self._discard_pooled_socket(sock)

                    if (
                        self._is_retryable_transport_error(exc)
                        and attempt < self.max_retries
                    ):
                        if self.retry_backoff_ms > 0:
                            time.sleep(self.retry_backoff_ms / 1000.0)
                        continue
                    raise
                finally:
                    # On the success path sock is still open; return it to pool.
                    # On the exception path _discard_pooled_socket already closed
                    # it, so fileno() == -1 and this is a no-op.
                    if sock.fileno() != -1:
                        self._pool_release(sock)
            raise RuntimeError("db client: retries exhausted") from last_exc

        with self.endpoint.connect(timeout_sec=self.timeout_sec) as tmp:
            send_message(tmp, payload)
            return recv_message(tmp)

    def query(self, hash: str) -> Optional[Tuple[int, str]]:
        resp = self._request({"op": "Query", "hash": hash})
        if resp.get("status") != "ok":
            raise RuntimeError(resp.get("error") or "db error")
        return resp.get("result")

    def command(self, id_: int, new_name_str: str) -> bool:
        resp = self._request(
            {"op": "Command", "id": int(id_), "new_name": new_name_str}
        )
        if resp.get("status") != "ok":
            raise RuntimeError(resp.get("error") or "db error")
        return bool(resp.get("result"))
