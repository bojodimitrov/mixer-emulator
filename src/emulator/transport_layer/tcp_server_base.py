import json
import os
import queue
import selectors
import socket
import struct
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Optional, Tuple

from ..metrics.runtime_metrics import now
from .transport import ProtocolError

_LEN_STRUCT = struct.Struct(">I")
_MAX_MESSAGE_BYTES = 10 * 1024 * 1024
_WAKEUP_TOKEN = "__wakeup__"
_DEFAULT_MAX_IN_BUFFER_BYTES = 4 * _MAX_MESSAGE_BYTES
_DEFAULT_MAX_PENDING_REQUESTS_PER_CONNECTION = 256
_DEFAULT_GLOBAL_PENDING_REQUESTS_MULTIPLIER = 8
_DEFAULT_MAX_QUEUED_OUTPUT_BYTES_PER_CONNECTION = 4 * 1024 * 1024
_DEFAULT_COMPLETION_QUEUE_MAXSIZE = 1024
_DEFAULT_RETRY_AFTER_MS = 250
_WINDOWS_SELECT_FD_LIMIT = 512
_WINDOWS_SELECT_RESERVED_FDS = 32


def _exc_to_message(exc: Exception) -> str:
    text = str(exc).strip()
    if text:
        return f"{type(exc).__name__}: {text}"
    return type(exc).__name__


@dataclass
class ConnState:
    in_buffer: bytearray = field(default_factory=bytearray)
    out_chunks: Deque[bytes] = field(default_factory=deque)
    queued_output_bytes: int = 0
    pending_requests: Deque[Dict[str, Any]] = field(default_factory=deque)
    in_flight: bool = False
    close_requested: bool = False
    close_after_write: bool = False
    last_activity: float = field(default_factory=time.monotonic)


class TcpServerBase(ABC):
    def __init__(
        self,
        *,
        host: str,
        port: int,
        max_connections: int,
        listen_backlog: Optional[int] = None,
        worker_pool_size: int,
        accept_timeout_sec: float,
        conn_timeout_sec: float,
        thread_name: str,
        worker_thread_prefix: str,
    ) -> None:
        self.host = str(host)
        self.port = int(port)
        self.accept_timeout_sec = float(accept_timeout_sec)
        self.conn_timeout_sec = float(conn_timeout_sec)
        requested_max_connections = int(max_connections)
        self.max_connections = requested_max_connections
        self._max_connections_was_clamped = False
        self._requested_max_connections = requested_max_connections
        if self.max_connections <= 0:
            raise ValueError("max_connections must be positive")

        # Windows SelectSelector is limited by FD_SETSIZE (typically 512).
        # Keep headroom for listen/wakeup sockets and transient bookkeeping.
        if os.name == "nt":
            safe_max_connections = max(
                1,
                _WINDOWS_SELECT_FD_LIMIT - _WINDOWS_SELECT_RESERVED_FDS,
            )
            if self.max_connections > safe_max_connections:
                self.max_connections = safe_max_connections
                self._max_connections_was_clamped = True

        if listen_backlog is None:
            self.listen_backlog = self.max_connections
        else:
            self.listen_backlog = int(listen_backlog)
        if self.listen_backlog <= 0:
            raise ValueError("listen_backlog must be positive")

        self.worker_pool_size = int(worker_pool_size)
        self._thread_name = thread_name
        self._worker_thread_prefix = worker_thread_prefix
        self.max_in_buffer_bytes_per_connection = _DEFAULT_MAX_IN_BUFFER_BYTES
        self.max_pending_requests_per_connection = (
            _DEFAULT_MAX_PENDING_REQUESTS_PER_CONNECTION
        )
        self.max_pending_requests_global = max(
            self.max_pending_requests_per_connection,
            self.worker_pool_size * _DEFAULT_GLOBAL_PENDING_REQUESTS_MULTIPLIER,
        )
        self.max_queued_output_bytes_per_connection = (
            _DEFAULT_MAX_QUEUED_OUTPUT_BYTES_PER_CONNECTION
        )
        self.completion_queue_maxsize = max(
            _DEFAULT_COMPLETION_QUEUE_MAXSIZE,
            self.worker_pool_size * 8,
        )

        self._socket: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._executor: Optional[ThreadPoolExecutor] = None
        self._selector: Optional[selectors.BaseSelector] = None
        self._wakeup_reader: Optional[socket.socket] = None
        self._wakeup_writer: Optional[socket.socket] = None
        self._completion_q: queue.Queue[Tuple[socket.socket, Dict[str, Any], float]] = (
            queue.Queue(maxsize=self.completion_queue_maxsize)
        )
        self._completion_overflow: set[socket.socket] = set()
        self._completion_overflow_lock = threading.Lock()
        self._global_pending_requests = 0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        if self.worker_pool_size <= 0:
            raise ValueError("worker_pool_size must be positive")

        self._executor = ThreadPoolExecutor(
            max_workers=self.worker_pool_size,
            thread_name_prefix=self._worker_thread_prefix,
        )

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((self.host, self.port))
        self._socket.listen(self.listen_backlog)
        self._socket.setblocking(False)

        self._selector = selectors.DefaultSelector()
        self._selector.register(self._socket, selectors.EVENT_READ, data=None)
        self._create_wakeup_channel()

        if self._max_connections_was_clamped:
            self._record_error(
                "max_connections clamped for Windows select() limit: "
                f"requested={self._requested_max_connections}, effective={self.max_connections}"
            )

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._serve,
            name=self._thread_name,
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        self._stop_event.set()
        self._signal_wakeup()

        if self._thread:
            self._thread.join(timeout=2)
            if self._thread.is_alive():
                self._record_error(
                    "server thread did not stop cleanly; deferring resource teardown"
                )
                return
            self._thread = None

        self._close_wakeup_channel()

        if self._selector is not None:
            try:
                self._selector.close()
            except OSError as exc:
                self._record_error(f"failed to close selector: {_exc_to_message(exc)}")
            self._selector = None

        if self._socket:
            try:
                self._socket.close()
            except OSError as exc:
                self._record_error(
                    f"failed to close server socket: {_exc_to_message(exc)}"
                )
            self._socket = None

        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None

        self._on_close()

    def _serve(self) -> None:
        assert self._socket is not None
        assert self._selector is not None

        while not self._stop_event.is_set():
            self._drain_completions()
            self._close_idle_connections()

            try:
                events = self._selector.select(timeout=self.accept_timeout_sec)
            except OSError as exc:
                if not self._stop_event.is_set():
                    self._record_error(f"selector loop failed: {_exc_to_message(exc)}")
                break
            except ValueError as exc:
                if not self._stop_event.is_set():
                    self._record_error(
                        "selector loop failed due to descriptor limit: "
                        f"{_exc_to_message(exc)}"
                    )
                break

            for key, mask in events:
                if key.data is None:
                    self._accept_ready_connections()
                    continue
                if key.data == _WAKEUP_TOKEN:
                    self._drain_wakeup_channel()
                    continue

                conn = key.fileobj
                state = key.data
                if not isinstance(conn, socket.socket) or not isinstance(
                    state, ConnState
                ):
                    continue

                if mask & selectors.EVENT_READ:
                    self._read_from_connection(conn, state)
                if mask & selectors.EVENT_WRITE:
                    self._flush_writes(conn, state)

        self._shutdown_all_connections()

    def _accept_ready_connections(self) -> None:
        assert self._socket is not None
        assert self._selector is not None

        while not self._stop_event.is_set():
            try:
                conn, _addr = self._socket.accept()
            except BlockingIOError:
                return
            except OSError as exc:
                if not self._stop_event.is_set():
                    self._record_error(f"accept failed: {_exc_to_message(exc)}")
                return

            conn.setblocking(False)
            if self._active_connection_count() >= self.max_connections:
                try:
                    conn.close()
                except OSError as exc:
                    self._record_error(
                        f"failed to close excess connection: {_exc_to_message(exc)}"
                    )
                continue

            self._on_connection_accepted(conn)
            self._selector.register(
                conn, selectors.EVENT_READ, data=self._make_conn_state()
            )

    def _read_from_connection(self, conn: socket.socket, state: ConnState) -> None:
        if state.close_requested or state.close_after_write:
            return

        while not self._stop_event.is_set():
            try:
                chunk = conn.recv(65536)
            except BlockingIOError:
                break
            except OSError as exc:
                self._record_error(
                    f"read failed; closing connection: {_exc_to_message(exc)}"
                )
                self._close_connection(conn)
                return

            if not chunk:
                self._close_connection(conn)
                return

            state.last_activity = time.monotonic()
            state.in_buffer.extend(chunk)
            if len(state.in_buffer) > self.max_in_buffer_bytes_per_connection:
                self._record_error(
                    "closing connection due to oversized buffered input: "
                    f"{len(state.in_buffer)} bytes"
                )
                state.in_buffer.clear()
                self._queue_overload_response(
                    conn,
                    state,
                    "request buffer limit exceeded",
                    close_after=True,
                )
                return

            try:
                messages = self._extract_messages(state)
            except ProtocolError as exc:
                self._record_error(f"protocol error while reading request: {str(exc)}")
                state.in_buffer.clear()
                self._queue_response(
                    conn,
                    state,
                    {"status": "error", "error": str(exc)},
                    close_after=True,
                )
                return

            for message in messages:
                if message.get("op") == "Close":
                    state.close_requested = True
                    self._release_global_pending_requests(len(state.pending_requests))
                    state.pending_requests.clear()
                    state.in_buffer.clear()
                    if not state.in_flight:
                        self._queue_response(
                            conn,
                            state,
                            {"status": "ok", "result": True},
                            close_after=True,
                        )
                    return

                outstanding = len(state.pending_requests) + (
                    1 if state.in_flight else 0
                )
                if outstanding >= self.max_pending_requests_per_connection:
                    self._record_error(
                        "closing connection due to outstanding request limit: "
                        f"{outstanding}"
                    )
                    self._queue_overload_response(
                        conn,
                        state,
                        "too many outstanding requests",
                        close_after=True,
                    )
                    return

                if not self._try_acquire_global_pending_request_slot():
                    self._record_error(
                        "rejecting request due to global backlog limit: "
                        f"limit={self.max_pending_requests_global}"
                    )
                    self._queue_overload_response(
                        conn,
                        state,
                        "server backlog limit exceeded",
                        close_after=False,
                    )
                    continue

                state.pending_requests.append(message)
                self._dispatch_next_request(conn, state)

    def _flush_writes(self, conn: socket.socket, state: ConnState) -> None:
        while state.out_chunks:
            payload = state.out_chunks[0]
            try:
                sent = conn.send(payload)
            except BlockingIOError:
                self._set_conn_events(conn, state)
                return
            except OSError as exc:
                self._record_error(
                    f"write failed; closing connection: {_exc_to_message(exc)}"
                )
                self._close_connection(conn)
                return

            state.last_activity = time.monotonic()
            state.queued_output_bytes -= sent
            if sent < len(payload):
                state.out_chunks[0] = payload[sent:]
                self._set_conn_events(conn, state)
                return

            state.out_chunks.popleft()

        if state.close_after_write:
            self._close_connection(conn)
            return

        self._set_conn_events(conn, state)

    def _extract_messages(self, state: ConnState) -> Deque[Dict[str, Any]]:
        messages: Deque[Dict[str, Any]] = deque()

        while True:
            if len(state.in_buffer) < _LEN_STRUCT.size:
                return messages

            (length,) = _LEN_STRUCT.unpack_from(state.in_buffer, 0)
            if length <= 0:
                raise ProtocolError("invalid message length")
            if length > _MAX_MESSAGE_BYTES:
                raise ProtocolError(f"message too large: {length} bytes")

            frame_size = _LEN_STRUCT.size + length
            if len(state.in_buffer) < frame_size:
                return messages

            payload = bytes(state.in_buffer[_LEN_STRUCT.size : frame_size])
            del state.in_buffer[:frame_size]

            try:
                msg = json.loads(payload.decode("utf-8"))
            except Exception as exc:
                raise ProtocolError("invalid json payload") from exc

            if not isinstance(msg, dict):
                raise ProtocolError("message must be a json object")

            messages.append(msg)

    def _dispatch_next_request(self, conn: socket.socket, state: ConnState) -> None:
        assert self._executor is not None

        if state.in_flight or not state.pending_requests or self._stop_event.is_set():
            return

        msg = state.pending_requests.popleft()
        started = now()
        state.in_flight = True

        def _run_request() -> Dict[str, Any]:
            try:
                return self._handle_request_message(msg)
            except Exception as exc:
                error_message = _exc_to_message(exc)
                self._record_error(
                    f"request handler failed: {self._request_context(msg)}: {error_message}"
                )
                return {"status": "error", "error": error_message}

        try:
            future = self._executor.submit(_run_request)
        except Exception as exc:
            state.in_flight = False
            dropped_pending = len(state.pending_requests)
            if dropped_pending:
                state.pending_requests.clear()
            self._release_global_pending_requests(1 + dropped_pending)
            error_message = _exc_to_message(exc)
            self._record_error(f"failed to submit request: {error_message}")
            self._queue_response(
                conn,
                state,
                {"status": "error", "error": error_message},
                close_after=True,
            )
            return

        def _on_done(done_future) -> None:
            try:
                reply = done_future.result()
            except Exception as exc:
                error_message = _exc_to_message(exc)
                self._record_error(
                    f"request future failed unexpectedly: {error_message}"
                )
                reply = {"status": "error", "error": error_message}
            try:
                self._completion_q.put_nowait((conn, reply, started))
            except queue.Full:
                self._record_error(
                    "completion queue is full; marking connection for overload response"
                )
                self._mark_completion_overflow(conn)
            self._signal_wakeup()

        future.add_done_callback(_on_done)

    def _drain_completions(self) -> None:
        for conn in self._take_completion_overflow_connections():
            state = self._get_conn_state(conn)
            if state is None:
                continue

            dropped_pending = len(state.pending_requests)
            self._release_global_pending_requests(1 + dropped_pending)
            state.in_flight = False
            state.pending_requests.clear()
            state.in_buffer.clear()
            self._queue_overload_response(
                conn,
                state,
                "server is overloaded",
                close_after=True,
            )

        while True:
            try:
                conn, reply, started = self._completion_q.get_nowait()
            except queue.Empty:
                return

            self._release_global_pending_requests(1)

            state = self._get_conn_state(conn)
            if state is None:
                continue

            state.in_flight = False
            self._on_completion(state, reply)
            self._emit_metrics(reply, started)
            self._queue_response(
                conn,
                state,
                reply,
                close_after=self._close_after_response(state, reply),
            )
            if state.close_requested:
                state.pending_requests.clear()
                self._queue_response(
                    conn,
                    state,
                    {"status": "ok", "result": True},
                    close_after=True,
                )
                continue
            self._dispatch_next_request(conn, state)

    def _queue_response(
        self,
        conn: socket.socket,
        state: ConnState,
        payload: Dict[str, Any],
        *,
        close_after: bool = False,
    ) -> None:
        try:
            encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        except Exception as exc:
            self._record_error(
                f"failed to serialize response payload: {_exc_to_message(exc)}"
            )
            encoded = b'{"status":"error","error":"invalid response"}'

        framed = _LEN_STRUCT.pack(len(encoded)) + encoded
        projected = state.queued_output_bytes + len(framed)
        if projected > self.max_queued_output_bytes_per_connection:
            self._record_error(
                "closing connection due to queued output limit: " f"{projected} bytes"
            )
            state.out_chunks.clear()
            state.queued_output_bytes = 0
            self._close_connection(conn)
            return

        state.out_chunks.append(framed)
        state.queued_output_bytes = projected
        if close_after:
            state.close_after_write = True

        state.last_activity = time.monotonic()
        self._set_conn_events(conn, state)

    def _queue_overload_response(
        self,
        conn: socket.socket,
        state: ConnState,
        detail: str,
        *,
        close_after: bool,
    ) -> None:
        self._queue_response(
            conn,
            state,
            {
                "status": "error",
                "code": 429,
                "error": "too many requests",
                "detail": detail,
                "retry_after_ms": _DEFAULT_RETRY_AFTER_MS,
            },
            close_after=close_after,
        )

    def _mark_completion_overflow(self, conn: socket.socket) -> None:
        with self._completion_overflow_lock:
            self._completion_overflow.add(conn)

    def _take_completion_overflow_connections(self) -> list[socket.socket]:
        with self._completion_overflow_lock:
            conns = list(self._completion_overflow)
            self._completion_overflow.clear()
        return conns

    def _try_acquire_global_pending_request_slot(self) -> bool:
        if self.max_pending_requests_global <= 0:
            return False
        if self._global_pending_requests >= self.max_pending_requests_global:
            return False
        self._global_pending_requests += 1
        return True

    def _release_global_pending_requests(self, count: int) -> None:
        if count <= 0:
            return
        self._global_pending_requests = max(0, self._global_pending_requests - count)

    def _set_conn_events(self, conn: socket.socket, state: ConnState) -> None:
        assert self._selector is not None
        events = selectors.EVENT_READ
        if state.out_chunks:
            events |= selectors.EVENT_WRITE

        try:
            self._selector.modify(conn, events, data=state)
        except OSError as exc:
            self._record_error(
                "failed to update selector events; "
                f"closing connection: {_exc_to_message(exc)}"
            )
            self._close_connection(conn)

    def _close_idle_connections(self) -> None:
        assert self._selector is not None

        cutoff = time.monotonic() - self.conn_timeout_sec
        to_close = []
        for key in self._selector.get_map().values():
            if key.data is None or not isinstance(key.fileobj, socket.socket):
                continue
            state = key.data
            if (
                isinstance(state, ConnState)
                and not state.in_flight
                and not state.out_chunks
                and not state.pending_requests
                and state.last_activity < cutoff
            ):
                to_close.append(key.fileobj)

        for conn in to_close:
            self._close_connection(conn)

    def _close_connection(self, conn: socket.socket) -> None:
        with self._completion_overflow_lock:
            self._completion_overflow.discard(conn)

        if self._selector is None:
            return

        state = self._get_conn_state(conn)
        if state is not None and state.pending_requests:
            self._release_global_pending_requests(len(state.pending_requests))
            state.pending_requests.clear()

        try:
            self._selector.unregister(conn)
        except Exception as exc:
            self._record_error(
                "failed to unregister connection from selector: "
                f"{_exc_to_message(exc)}"
            )

        try:
            conn.close()
        except OSError as exc:
            self._record_error(
                f"failed to close connection socket: {_exc_to_message(exc)}"
            )

    def _get_conn_state(self, conn: socket.socket) -> Optional[ConnState]:
        if self._selector is None:
            return None
        try:
            key = self._selector.get_key(conn)
        except (KeyError, ValueError):
            return None

        if isinstance(key.data, ConnState):
            return key.data
        return None

    def _shutdown_all_connections(self) -> None:
        if self._selector is None:
            return

        sockets = [
            key.fileobj
            for key in self._selector.get_map().values()
            if isinstance(key.fileobj, socket.socket) and key.data is not None
        ]
        for conn in sockets:
            self._close_connection(conn)

    def _create_wakeup_channel(self) -> None:
        if self._selector is None:
            return
        reader, writer = socket.socketpair()
        reader.setblocking(False)
        writer.setblocking(False)
        self._wakeup_reader = reader
        self._wakeup_writer = writer
        self._selector.register(reader, selectors.EVENT_READ, data=_WAKEUP_TOKEN)

    def _close_wakeup_channel(self) -> None:
        if self._selector is not None and self._wakeup_reader is not None:
            try:
                self._selector.unregister(self._wakeup_reader)
            except Exception as exc:
                self._record_error(
                    f"failed to unregister wakeup socket: {_exc_to_message(exc)}"
                )

        for sock in (self._wakeup_reader, self._wakeup_writer):
            if sock is None:
                continue
            try:
                sock.close()
            except OSError as exc:
                self._record_error(
                    f"failed to close wakeup socket: {_exc_to_message(exc)}"
                )

        self._wakeup_reader = None
        self._wakeup_writer = None

    def _signal_wakeup(self) -> None:
        if self._wakeup_writer is None:
            return
        try:
            self._wakeup_writer.send(b"\x00")
        except BlockingIOError:
            # The wakeup socket is already full, which implies the selector
            # will wake soon anyway.
            return
        except OSError as exc:
            if not self._stop_event.is_set():
                self._record_error(
                    f"failed to signal wakeup channel: {_exc_to_message(exc)}"
                )

    def _active_connection_count(self) -> int:
        if self._selector is None:
            return 0

        active = 0
        for key in self._selector.get_map().values():
            if isinstance(key.fileobj, socket.socket) and isinstance(
                key.data, ConnState
            ):
                active += 1
        return active

    def _drain_wakeup_channel(self) -> None:
        if self._wakeup_reader is None:
            return
        while True:
            try:
                chunk = self._wakeup_reader.recv(4096)
            except BlockingIOError:
                return
            except OSError as exc:
                if not self._stop_event.is_set():
                    self._record_error(
                        f"failed to drain wakeup channel: {_exc_to_message(exc)}"
                    )
                return
            if not chunk:
                return

    def _emit_metrics(self, resp: Dict[str, Any], started: float) -> None:
        ok = isinstance(resp, dict) and resp.get("status") == "ok"
        self._record_metric(ok, (now() - started) * 1000.0)

    def _make_conn_state(self) -> ConnState:
        return ConnState()

    def _on_connection_accepted(self, conn: socket.socket) -> None:
        return

    def _on_completion(self, state: ConnState, reply: Dict[str, Any]) -> None:
        return

    def _close_after_response(self, state: ConnState, reply: Dict[str, Any]) -> bool:
        return False

    def _request_context(self, msg: Dict[str, Any]) -> str:
        return str(msg.get("op", "unknown"))

    def _on_close(self) -> None:
        return

    @abstractmethod
    def _handle_request_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def _record_error(self, message: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def _record_metric(self, ok: bool, latency_ms: float) -> None:
        raise NotImplementedError
