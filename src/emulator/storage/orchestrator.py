from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional

from .engine import DbEngine

OperationType = Literal["Query", "GetById", "Command", "CommandWithHashes"]


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

    if request.operation == "GetById":
        id_ = request.payload["id"]
        return db.get_by_id(id_)

    if request.operation == "Command":
        id_ = request.payload["id"]
        new_name_str = request.payload["new_name"]
        return db.command_update_record(id_, new_name_str)

    if request.operation == "CommandWithHashes":
        id_ = request.payload["id"]
        new_name_str = request.payload["new_name"]
        updated, old_hash, new_hash = db.command_update_record_with_hashes(id_, new_name_str)
        return {"updated": updated, "old_hash": old_hash, "new_hash": new_hash}

    raise ValueError(f"unknown operation: {request.operation}")


class DbOrchestrator:
    """
    Thread-pool database orchestrator.

    Incoming requests are executed by a fixed worker pool against a shared
    DbEngine instance to avoid per-request process and DB initialization overhead.
    """

    def __init__(
        self,
        timeout_sec: float = 30.0,
        pool_size: int = 64,
        db_path: Optional[str] = None,
        shard_index: int = 0,
        shard_count: int = 1,
    ):
        if pool_size <= 0:
            raise ValueError("pool_size must be positive")

        self.timeout_sec = float(timeout_sec)
        self._db = DbEngine(
            db_path=db_path,
            shard_index=shard_index,
            shard_count=shard_count,
        )
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
