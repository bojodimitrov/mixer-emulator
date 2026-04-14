from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from dataclasses import dataclass
from typing import Any, Dict, Literal

from .engine import DbEngine

LookupStrategy = Literal["linear", "bplus"]
OperationType = Literal["Query", "Command"]


@dataclass
class DbRequest:
    operation: OperationType
    payload: Dict[str, Any]


def _database_worker(
    db: DbEngine,
    request: DbRequest,
) -> Any:
    if request.operation == "Query":
        if db.record_count() == 0:
            raise RuntimeError(
                "database is empty; build it first with `python -m emulator.storage.database`"
            )

        hash_hex = request.payload["hash"]
        return db.query_by_hash(hash_hex)

    if request.operation == "Command":
        id_ = request.payload["id"]
        new_name_str = request.payload["new_name"]
        return db.command_update_record(id_, new_name_str)

    raise ValueError(f"unknown operation: {request.operation}")


class DbOrchestrator:
    """
    Thread-pool database orchestrator.

    Incoming requests are executed by a fixed worker pool against a shared
    DbEngine instance to avoid per-request process and DB initialization overhead.
    """

    def __init__(
        self,
        lookup_strategy: LookupStrategy = DbEngine.STRATEGY_LINEAR,
        timeout_sec: float = 30.0,
        pool_size: int = 50,
    ):
        if pool_size <= 0:
            raise ValueError("pool_size must be positive")

        self.lookup_strategy = lookup_strategy
        self.timeout_sec = float(timeout_sec)
        self._db = DbEngine(lookup_strategy=lookup_strategy)
        self._executor = ThreadPoolExecutor(
            max_workers=int(pool_size),
            thread_name_prefix="db-server",
        )

    def handle_request(self, request: DbRequest) -> Any:
        try:
            future = self._executor.submit(_database_worker, self._db, request)
            return future.result(timeout=self.timeout_sec)
        except FuturesTimeout as exc:
            raise TimeoutError(
                f"database request timed out after {self.timeout_sec:.1f}s"
            ) from exc

    def close(self) -> None:
        self._executor.shutdown(wait=True)
