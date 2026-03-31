from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from dataclasses import dataclass
from typing import Any, Dict, Literal

from .database import FileDB

LookupStrategy = Literal["linear", "sorted", "bplus"]
OperationType = Literal["Query", "Command"]


@dataclass
class DatabaseRequest:
    operation: OperationType
    payload: Dict[str, Any]


def _database_worker(
    db: FileDB,
    request: DatabaseRequest,
) -> Any:
    if request.operation == "Query":
        if db.record_count() == 0:
            raise RuntimeError(
                "database is empty; build it first with `python -m emulator.storage.database`"
            )

        hash_bytes = request.payload["hash_bytes"]
        return db.query_by_hash(hash_bytes)

    if request.operation == "Command":
        id_ = request.payload["id"]
        new_name_str = request.payload["new_name"]
        return db.update_record(id_, new_name_str)

    raise ValueError(f"unknown operation: {request.operation}")


class DatabaseServer:
    """
    Thread-pool database server.

    Incoming requests are executed by a fixed worker pool against a shared
    FileDB instance to avoid per-request process and DB initialization overhead.
    """

    def __init__(
        self,
        lookup_strategy: LookupStrategy = FileDB.STRATEGY_LINEAR,
        timeout_sec: float = 30.0,
        pool_size: int = 50,
    ):
        if lookup_strategy not in FileDB.LOOKUP_STRATEGIES:
            supported = ", ".join(sorted(FileDB.LOOKUP_STRATEGIES))
            raise ValueError(
                f"unsupported lookup_strategy={lookup_strategy!r}; expected one of: {supported}"
            )
        if pool_size <= 0:
            raise ValueError("pool_size must be positive")

        self.lookup_strategy = lookup_strategy
        self.timeout_sec = float(timeout_sec)
        self._db = FileDB(lookup_strategy=lookup_strategy)
        self._executor = ThreadPoolExecutor(
            max_workers=int(pool_size),
            thread_name_prefix="db-server",
        )

    def handle_request(self, request: DatabaseRequest) -> Any:
        try:
            future = self._executor.submit(_database_worker, self._db, request)
            return future.result(timeout=self.timeout_sec)
        except FuturesTimeout as exc:
            raise TimeoutError(
                f"database request timed out after {self.timeout_sec:.1f}s"
            ) from exc

    def close(self) -> None:
        self._executor.shutdown(wait=True)
