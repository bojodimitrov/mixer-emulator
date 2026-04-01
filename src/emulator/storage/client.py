import queue
import socket
import threading
from contextlib import suppress
from typing import Optional, Tuple

from ..servers_config import DB_ENDPOINT
from ..transport_layer.transport import (
    TcpEndpoint,
    recv_message,
    send_message,
    hex_from_bytes,
)


class DbClient:
    """Database client that talks to `DbServer` over TCP."""

    def __init__(
        self,
        timeout_sec: float = 5.0,
        keepalive: bool = False,
        pool_size: int = 0,
    ):
        self.endpoint = TcpEndpoint(DB_ENDPOINT.host, int(DB_ENDPOINT.port))
        self.timeout_sec = float(timeout_sec)
        self.keepalive = bool(keepalive)

        # Connection management modes:
        # - default (pool_size==0 and keepalive==False): open a new TCP connection per request
        # - keepalive: one shared TCP connection reused by all calls on this instance (NOT thread-safe)
        # - pool_size>0: a pool of reusable TCP connections (thread-safe)
        if pool_size < 0:
            raise ValueError("pool_size must be >= 0")
        if self.keepalive and pool_size > 0:
            raise ValueError("use either keepalive or pool_size, not both")
        self.pool_size = int(pool_size)

        self._socket = None  # keepalive-only

        self._pool: Optional[queue.LifoQueue[socket.socket]] = None
        self._pool_lock = threading.Lock()
        self._created = 0
        if self.pool_size > 0:
            self._pool = queue.LifoQueue(maxsize=self.pool_size)

            # Eagerly create half the pool to reduce first-request connect overhead,
            # without paying the cost of creating the full pool up-front.
            eager = max(0, self.pool_size // 2)
            for _ in range(eager):
                s = self.endpoint.connect(timeout_sec=self.timeout_sec)
                self._created += 1
                self._pool.put_nowait(s)

    def close(self) -> None:
        socket_ = self._socket
        self._socket = None
        if socket_ is not None:
            with suppress(Exception):
                send_message(socket_, {"op": "Close"})
            with suppress(Exception):
                socket_.close()

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

    def _get_socket(self):
        if not self.keepalive:
            return None
        if self._socket is None:
            self._socket = self.endpoint.connect(timeout_sec=self.timeout_sec)
        return self._socket

    def _pool_acquire(self) -> socket.socket:
        pool = self._pool
        if pool is None:
            raise RuntimeError("pool not enabled")

        try:
            sock = pool.get_nowait()
            return sock
        except queue.Empty:
            pass

        with self._pool_lock:
            if self._created < self.pool_size:
                self._created += 1
                return self.endpoint.connect(timeout_sec=self.timeout_sec)

        # Pool is at capacity; wait for a socket to be returned.
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

    def _request(self, payload: dict):
        # Pool mode (thread-safe reuse, keep-alive protocol per socket).
        if self._pool is not None:
            socket = self._pool_acquire()
            try:
                send_message(socket, payload)
                return recv_message(socket)
            except Exception:
                # Drop broken sockets so the pool can recreate later.
                with suppress(Exception):
                    socket.close()
                with self._pool_lock:
                    self._created = max(0, self._created - 1)
                raise
            finally:
                # If the socket is already closed, releasing is harmless.
                if socket.fileno() != -1:
                    self._pool_release(socket)

        socket = self._get_socket()
        if socket is None:
            with self.endpoint.connect(timeout_sec=self.timeout_sec) as tmp:
                send_message(tmp, payload)
                return recv_message(tmp)
        send_message(socket, payload)
        return recv_message(socket)

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
