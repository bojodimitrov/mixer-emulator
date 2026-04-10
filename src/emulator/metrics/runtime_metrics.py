from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict


def now() -> float:
    return time.perf_counter()


@dataclass
class Counter:
    total: int = 0
    ok: int = 0
    error: int = 0
    last_latency_ms: float = 0.0


@dataclass
class ErrorEvent:
    ts_sec: float
    source: str
    message: str


@dataclass
class TransientEvent:
    ts_sec: float
    source: str
    message: str
    count: int


class RuntimeMetrics:
    def __init__(self) -> None:
        self.started_at = now()
        self._lock = threading.Lock()
        self.db = Counter()
        self.service = Counter()
        self.client_corrupter = Counter()
        self.client_repairer = Counter()
        self._recent_errors: Deque[ErrorEvent] = deque(maxlen=5000)
        self._recent_transients: Deque[TransientEvent] = deque(maxlen=5000)
        self._transient_counts: Dict[str, int] = {
            "corrupter": 0,
            "repairer": 0,
            "service": 0,
            "db": 0,
            "metrics": 0,
            "other": 0,
        }

    def record_db(self, ok: bool, latency_ms: float) -> None:
        self._record(self.db, ok, latency_ms)

    def record_service(self, ok: bool, latency_ms: float) -> None:
        self._record(self.service, ok, latency_ms)

    def record_client(self, kind: str, ok: bool, latency_ms: float) -> None:
        target = self.client_corrupter if kind == "corrupter" else self.client_repairer
        self._record(target, ok, latency_ms)

    def record_error(self, source: str, message: str) -> None:
        with self._lock:
            self._recent_errors.append(
                ErrorEvent(ts_sec=now(), source=str(source), message=str(message))
            )

    def record_transient(self, source: str, message: str, count: int = 1) -> None:
        normalized_source = str(source)
        bucket = (
            normalized_source
            if normalized_source in self._transient_counts
            else "other"
        )
        safe_count = max(1, int(count))
        with self._lock:
            self._transient_counts[bucket] += safe_count
            self._recent_transients.append(
                TransientEvent(
                    ts_sec=now(),
                    source=normalized_source,
                    message=str(message),
                    count=safe_count,
                )
            )

    def _record(self, counter: Counter, ok: bool, latency_ms: float) -> None:
        with self._lock:
            counter.total += 1
            if ok:
                counter.ok += 1
            else:
                counter.error += 1
            counter.last_latency_ms = latency_ms

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            up_sec = max(1e-9, now() - self.started_at)
            return {
                "uptime_sec": up_sec,
                "db": self._counter_snapshot(self.db, up_sec),
                "service": self._counter_snapshot(self.service, up_sec),
                "corrupter": self._counter_snapshot(self.client_corrupter, up_sec),
                "repairer": self._counter_snapshot(self.client_repairer, up_sec),
                "recent_errors": [
                    {
                        "ts_sec": e.ts_sec,
                        "source": e.source,
                        "message": e.message,
                    }
                    for e in list(self._recent_errors)
                ],
                "transient": {
                    "counts": dict(self._transient_counts),
                    "recent": [
                        {
                            "ts_sec": e.ts_sec,
                            "source": e.source,
                            "message": e.message,
                            "count": e.count,
                        }
                        for e in list(self._recent_transients)
                    ],
                },
            }

    @staticmethod
    def _counter_snapshot(counter: Counter, up_sec: float) -> Dict[str, Any]:
        return {
            "total": counter.total,
            "ok": counter.ok,
            "error": counter.error,
            "last_latency_ms": counter.last_latency_ms,
            "ops_per_sec": counter.total / up_sec,
        }
