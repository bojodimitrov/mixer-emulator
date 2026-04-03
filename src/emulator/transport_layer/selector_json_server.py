import json
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


def _exc_to_message(exc: Exception) -> str:
    text = str(exc).strip()
    if text:
        return f"{type(exc).__name__}: {text}"
    return type(exc).__name__


@dataclass
class ConnState:
    in_buffer: bytearray = field(default_factory=bytearray)
    out_chunks: Deque[bytes] = field(default_factory=deque)
    pending_requests: Deque[Dict[str, Any]] = field(default_factory=deque)
    in_flight: bool = False
    handled: int = 0
    close_after_write: bool = False
    last_activity: float = field(default_factory=time.monotonic)


class SelectorJsonServer(ABC):
    def __init__(
        self,
        *,
        host: str,
        port: int,
        max_connections: int,
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
        self.max_connections = int(max_connections)
        self.worker_pool_size = int(worker_pool_size)
        self._thread_name = thread_name
        self._worker_thread_prefix = worker_thread_prefix

        self._socket: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._executor: Optional[ThreadPoolExecutor] = None
        self._selector: Optional[selectors.BaseSelector] = None
        self._wakeup_reader: Optional[socket.socket] = None
        self._wakeup_writer: Optional[socket.socket] = None
        self._completion_q: queue.Queue[Tuple[socket.socket, Dict[str, Any], float]] = (
            queue.Queue()
        )

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
        self._socket.listen(self.max_connections)
        self._socket.setblocking(False)

        self._selector = selectors.DefaultSelector()
        self._selector.register(self._socket, selectors.EVENT_READ, data=None)
        self._create_wakeup_channel()

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
            self._on_connection_accepted(conn)
            self._selector.register(
                conn, selectors.EVENT_READ, data=self._make_conn_state()
            )

    def _read_from_connection(self, conn: socket.socket, state: ConnState) -> None:
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

            try:
                messages = self._extract_messages(state)
            except ProtocolError as exc:
                self._record_error(f"protocol error while reading request: {str(exc)}")
                self._queue_response(
                    conn, state, {"status": "error", "error": str(exc)}
                )
                continue

            for message in messages:
                if message.get("op") == "Close":
                    self._queue_response(
                        conn,
                        state,
                        {"status": "ok", "result": True},
                        close_after=True,
                    )
                    return

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

        future = self._executor.submit(_run_request)

        def _on_done(done_future) -> None:
            try:
                reply = done_future.result()
            except Exception as exc:
                error_message = _exc_to_message(exc)
                self._record_error(
                    f"request future failed unexpectedly: {error_message}"
                )
                reply = {"status": "error", "error": error_message}
            self._completion_q.put((conn, reply, started))
            self._signal_wakeup()

        future.add_done_callback(_on_done)

    def _drain_completions(self) -> None:
        while True:
            try:
                conn, reply, started = self._completion_q.get_nowait()
            except queue.Empty:
                return

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

        state.out_chunks.append(_LEN_STRUCT.pack(len(encoded)) + encoded)
        if close_after:
            state.close_after_write = True

        state.last_activity = time.monotonic()
        self._set_conn_events(conn, state)

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
            if isinstance(state, ConnState) and state.last_activity < cutoff:
                to_close.append(key.fileobj)

        for conn in to_close:
            self._close_connection(conn)

    def _close_connection(self, conn: socket.socket) -> None:
        if self._selector is None:
            return

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
        except OSError as exc:
            if not self._stop_event.is_set():
                self._record_error(
                    f"failed to signal wakeup channel: {_exc_to_message(exc)}"
                )

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
