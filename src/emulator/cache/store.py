from __future__ import annotations

import threading
import time
from typing import Any, Optional


class CacheStore:
    """Thread-safe in-memory cache with optional per-key TTL."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._values: dict[str, tuple[Any, Optional[float]]] = {}

    def set(self, key: str, value: Any, ttl_sec: Optional[float] = None) -> bool:
        expires_at: Optional[float] = None
        if ttl_sec is not None:
            ttl_sec = float(ttl_sec)
            if ttl_sec <= 0:
                raise ValueError("ttl_sec must be > 0 or None")
            expires_at = time.monotonic() + ttl_sec

        with self._lock:
            self._values[key] = (value, expires_at)
        return True

    def get(self, key: str) -> Any:
        with self._lock:
            return self._get_unlocked(key)

    def exists(self, key: str) -> bool:
        with self._lock:
            return self._get_unlocked(key) is not None

    def mget(self, keys: list[str]) -> list[Any]:
        with self._lock:
            return [self._get_unlocked(key) for key in keys]

    def incr(self, key: str, amount: int = 1) -> int:
        with self._lock:
            current = self._get_unlocked(key)
            if current is None:
                next_value = int(amount)
            else:
                if isinstance(current, bool) or not isinstance(current, int):
                    raise ValueError("INCR requires an integer value")
                next_value = current + int(amount)

            self._values[key] = (next_value, None)
            return next_value

    def delete(self, key: str) -> bool:
        with self._lock:
            if self._get_unlocked(key) is None:
                return False

            self._values.pop(key, None)
            return True

    def clear(self) -> None:
        with self._lock:
            self._values.clear()

    def _get_unlocked(self, key: str) -> Any:
        entry = self._values.get(key)
        if entry is None:
            return None

        value, expires_at = entry
        if expires_at is not None and expires_at <= time.monotonic():
            self._values.pop(key, None)
            return None
        return value