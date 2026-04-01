import json
import socket
import struct
from dataclasses import dataclass
from typing import Any, Dict, Optional


_LEN_STRUCT = struct.Struct(">I")  # 4-byte big-endian length prefix


class ProtocolError(RuntimeError):
    pass


def _recvall(socket: socket.socket, n: int) -> bytes:
    data = bytearray()
    while len(data) < n:
        chunk = socket.recv(n - len(data))
        if not chunk:
            raise ConnectionError("socket closed while reading")
        data.extend(chunk)
    return bytes(data)


def send_message(socket: socket.socket, message: Dict[str, Any]) -> None:
    payload = json.dumps(message, separators=(",", ":")).encode("utf-8")
    socket.sendall(_LEN_STRUCT.pack(len(payload)) + payload)


def recv_message(socket: socket.socket, *, max_bytes: int = 10 * 1024 * 1024) -> Dict[str, Any]:
    header = _recvall(socket, _LEN_STRUCT.size)
    (length,) = _LEN_STRUCT.unpack(header)
    if length <= 0:
        raise ProtocolError("invalid message length")
    if length > max_bytes:
        raise ProtocolError(f"message too large: {length} bytes")
    payload = _recvall(socket, length)
    try:
        obj = json.loads(payload.decode("utf-8"))
    except Exception as exc:
        raise ProtocolError("invalid json payload") from exc
    if not isinstance(obj, dict):
        raise ProtocolError("message must be a json object")
    return obj


def hex_from_bytes(b: bytes) -> str:
    return b.hex()


def bytes_from_hex(s: str) -> bytes:
    try:
        return bytes.fromhex(s)
    except Exception as exc:
        raise ValueError("invalid hex string") from exc


@dataclass(frozen=True)
class TcpEndpoint:
    host: str = "127.0.0.1"
    port: int = 0

    def connect(self, *, timeout_sec: float = 5.0) -> socket.socket:
        sock = socket.create_connection((self.host, self.port), timeout=timeout_sec)
        sock.settimeout(timeout_sec)
        return sock
