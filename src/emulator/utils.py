import hashlib
import time
from typing import Callable, TypeVar


T = TypeVar("T")


def id_to_name(idx: int) -> bytes:
    result = bytearray(5)
    n = idx
    for i in range(4, -1, -1):
        result[i] = ord("a") + (n % 26)
        n //= 26
    return bytes(result)


def compute_hash_for(id_: int, name: bytes) -> bytes:
    s = f"{id_}:{name.decode('ascii')}".encode("utf-8")
    return hashlib.sha256(s).digest()


def print_time(message: str, operation: Callable[[], T]) -> T:
    start = time.perf_counter()
    result = operation()
    elapsed_ms = (time.perf_counter() - start) * 1000
    print(f"({elapsed_ms:.3f} ms) ==> {message} -> {result} ")
    return result
