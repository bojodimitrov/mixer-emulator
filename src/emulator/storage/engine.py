import argparse
import errno
import threading
from contextlib import contextmanager
import mmap
import os
import struct
import time
from typing import Callable, Dict, Literal, Optional, Tuple

from emulator.transport_layer.transport import hex_from_bytes

from ..utils import compute_hash_for, id_to_name, print_time
from .b_tree_index import BPlusTreeIndex
from .constants import (
    RECORD_STRUCT,
    RECORD_SIZE,
    DB_HASH_OFFSET,
    DEFAULT_BPLUS_INDEX_PATH,
    DEFAULT_DB_PATH,
    DB_RECORD_SIZE,
)

# ── Module-level stripe locks ─────────────────────────────────────────────────
# Threading locks for in-process row-level mutual exclusion.  256 stripes means
# only rows that share the same stripe (id % 256) serialise; all other rows run
# fully in parallel.  File-level flock is kept only for bulk startup operations
# (ensure_capacity, populate_range) which may race with external processes.
_ROW_LOCK_STRIPES = 256
_row_stripe_locks: list[threading.Lock] = [
    threading.Lock() for _ in range(_ROW_LOCK_STRIPES)
]
_LOCK_RETRY_ATTEMPTS = 50
_LOCK_RETRY_DELAY_SEC = 0.01


class DbEngine:
    STRATEGY_LINEAR = "linear"
    STRATEGY_BPLUS = "bplus"
    LOOKUP_STRATEGIES = {
        STRATEGY_LINEAR,
        STRATEGY_BPLUS,
    }

    """
    Fixed-size-record file DB using mmap + struct.

    Record format (per row):
      - id: unsigned 64-bit (Q)
      - name: exactly 5 ASCII lower-case letters (5s)
      - hash: sha256(id:name) digest (32s)

    Hash algorithm used: sha256(f"{id}:{name}".encode("utf-8")).digest()

    Notes:
    - Rows are addressed by id (0-based). The file size determines number of rows.
    - You can pre-allocate capacity with ensure_capacity(capacity).
    - populate_range will write computed name/hash for ids in [start, end).
    - query_by_hash scans the file and returns (id, name) on match or None.
    - Generating all 11_881_376 rows will produce a ~535 MB file; populating will take time.
    """

    def __init__(
        self,
        lookup_strategy: Literal["linear", "bplus"] = STRATEGY_LINEAR,
    ):
        if lookup_strategy not in self.LOOKUP_STRATEGIES:
            supported = ", ".join(sorted(self.LOOKUP_STRATEGIES))
            raise ValueError(
                f"unsupported lookup_strategy={lookup_strategy!r}; expected one of: {supported}"
            )

        self.lookup_strategy = lookup_strategy
        os.makedirs(os.path.dirname(DEFAULT_DB_PATH) or ".", exist_ok=True)
        open(DEFAULT_DB_PATH, "a").close()
        self._lock_path = f"{DEFAULT_DB_PATH}.lock"
        open(self._lock_path, "a").close()

    @contextmanager
    def _read_lock(self):
        with open(self._lock_path, "r+b") as lock_file:
            self._lock_file(lock_file, shared=True)
            try:
                yield
            finally:
                self._unlock_file(lock_file)

    @contextmanager
    def _write_lock(self):
        with open(self._lock_path, "r+b") as lock_file:
            self._lock_file(lock_file, shared=False)
            try:
                yield
            finally:
                self._unlock_file(lock_file)

    @staticmethod
    def _lock_file(lock_file, shared: bool) -> None:
        # Some runtimes can transiently raise EDEADLK while another thread in the
        # same process is releasing a byte-range lock. Retry briefly before failing.
        retryable_errnos = {
            errno.EDEADLK,
            errno.EACCES,
            errno.EAGAIN,
        }
        if os.name == "nt":
            import msvcrt

            mode = msvcrt.LK_RLCK if shared else msvcrt.LK_LOCK
            for attempt in range(_LOCK_RETRY_ATTEMPTS):
                try:
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), mode, 1)
                    return
                except OSError as exc:
                    if (
                        exc.errno in retryable_errnos
                        and attempt < _LOCK_RETRY_ATTEMPTS - 1
                    ):
                        time.sleep(_LOCK_RETRY_DELAY_SEC)
                        continue
                    raise
            return

        import fcntl

        mode = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        for attempt in range(_LOCK_RETRY_ATTEMPTS):
            try:
                fcntl.flock(lock_file.fileno(), mode)
                return
            except OSError as exc:
                if exc.errno in retryable_errnos and attempt < _LOCK_RETRY_ATTEMPTS - 1:
                    time.sleep(_LOCK_RETRY_DELAY_SEC)
                    continue
                raise

    @staticmethod
    def _unlock_file(lock_file) -> None:
        if os.name == "nt":
            import msvcrt

            lock_file.seek(0)
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            return

        import fcntl

        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    # ── Row-level locking ────────────────────────────────────────────────────
    # Threading stripe locks allow concurrent access to different rows while
    # serialising access to the same row stripe within a process.

    @contextmanager
    def _row_read_lock(self, id_: int):
        """Acquire the stripe lock for reading row id_."""
        with _row_stripe_locks[id_ % _ROW_LOCK_STRIPES]:
            yield

    @contextmanager
    def _row_write_lock(self, id_: int):
        """Acquire the stripe lock for writing row id_."""
        with _row_stripe_locks[id_ % _ROW_LOCK_STRIPES]:
            yield

    @staticmethod
    def _validate_name(name_str: str, field_name: str = "name_str") -> None:
        if (
            not isinstance(name_str, str)
            or len(name_str) != 5
            or not name_str.isalpha()
            or not name_str.islower()
        ):
            raise ValueError(f"{field_name} must be a 5-letter lowercase ASCII word")

    def record_count(self) -> int:
        # DbServer owns the file exclusively at runtime; no cross-process lock needed.
        return os.path.getsize(DEFAULT_DB_PATH) // RECORD_SIZE

    def ensure_capacity(self, capacity: int) -> None:
        target = capacity * RECORD_SIZE
        with self._write_lock():
            with open(DEFAULT_DB_PATH, "ab") as f:
                f.tell()

            cur = os.path.getsize(DEFAULT_DB_PATH)
            if cur < target:
                with open(DEFAULT_DB_PATH, "r+b") as f:
                    f.truncate(target)

    def populate_range(self, start: int, end: int, progress_callback: Optional[callable] = None) -> None:  # type: ignore
        total = end - start
        with self._write_lock():
            with open(DEFAULT_DB_PATH, "r+b") as f:
                mm = mmap.mmap(f.fileno(), 0)
                try:
                    for i, id_ in enumerate(range(start, end), 1):
                        name = id_to_name(id_)
                        hashb = compute_hash_for(id_, name)
                        packed = struct.pack(RECORD_STRUCT, id_, name, hashb)
                        off = id_ * RECORD_SIZE
                        mm[off : off + RECORD_SIZE] = packed
                        if progress_callback and (i % 10000 == 0 or i == total):
                            progress_callback(i, total)
                finally:
                    mm.close()

    def read_record(self, id_: int) -> Tuple[int, str, str]:
        with self._row_read_lock(id_):
            file_size = os.path.getsize(DEFAULT_DB_PATH)
            if id_ * RECORD_SIZE + RECORD_SIZE > file_size:
                raise IndexError("id out of range")
            with open(DEFAULT_DB_PATH, "rb") as f:
                f.seek(id_ * RECORD_SIZE)
                data = f.read(RECORD_SIZE)

        id_read, name_b, hashb = struct.unpack(RECORD_STRUCT, data)
        return id_read, name_b.decode("ascii"), hex_from_bytes(hashb)

    def count_corrupted_records(
        self,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Count rows where stored name differs from canonical id_to_name(id)."""
        # No file lock needed: DbServer owns the file; slight staleness is
        # acceptable for metrics scans.
        file_size = os.path.getsize(DEFAULT_DB_PATH)

        if file_size == 0:
            if progress_callback is not None:
                progress_callback(0, 0)
            return 0

        corrupted = 0
        with open(DEFAULT_DB_PATH, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                total_records = len(mm) // RECORD_SIZE
                for index, off in enumerate(range(0, len(mm), RECORD_SIZE), 1):
                    id_, name_b, _ = struct.unpack_from(RECORD_STRUCT, mm, off)
                    if name_b != id_to_name(int(id_)):
                        corrupted += 1
                    if progress_callback is not None and (
                        index % 100_000 == 0 or index == total_records
                    ):
                        progress_callback(index, total_records)
            finally:
                mm.close()

        return corrupted

    def get_corruption_level(
        self,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> Dict[str, float | int]:
        """Return corruption metrics for the whole DB."""
        total_records = self.record_count()
        corrupted_records = self.count_corrupted_records(progress_callback=progress_callback)
        corruption_percent = (
            (corrupted_records / total_records) if total_records > 0 else 0.0
        ) * 100.0

        return {
            "total_records": total_records,
            "corrupted_records": corrupted_records,
            "corruption_percent": round(corruption_percent, 2),
        }

    def query_by_hash(self, hash: str) -> Optional[Tuple[int, str]]:
        if self.lookup_strategy == self.STRATEGY_LINEAR:
            return self._query_by_hash_linear(hash)
        return self._query_by_hash_bplus(hash)

    def update_record(self, id_: int, new_name_str: str) -> bool:
        if self.lookup_strategy == self.STRATEGY_LINEAR:
            return bool(self._command_update_record_linear(id_, new_name_str))
        return bool(self._command_update_record_bplus(id_, new_name_str))

    def command_update_record(self, id_: int, new_name_str: str) -> int:
        if self.lookup_strategy == self.STRATEGY_LINEAR:
            return self._command_update_record_linear(id_, new_name_str)
        return self._command_update_record_bplus(id_, new_name_str)

    def _query_by_hash_linear(self, hash: str) -> Optional[Tuple[int, str]]:
        hash_bytes = bytes.fromhex(hash)

        # No file lock: DbServer owns the file; concurrent writes are serialised
        # per-row by stripe locks, making a file-wide LOCK_SH unnecessary.
        file_size = os.path.getsize(DEFAULT_DB_PATH)

        if file_size == 0:
            return None

        with open(DEFAULT_DB_PATH, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                for off in range(0, len(mm), RECORD_SIZE):
                    hb = mm[off + DB_HASH_OFFSET : off + DB_RECORD_SIZE]
                    if hb == hash_bytes:
                        id_, name_b, _ = struct.unpack_from(RECORD_STRUCT, mm, off)
                        return int(id_), name_b.decode("ascii")
                return None
            finally:
                mm.close()

    def _query_by_hash_bplus(self, hash: str) -> Optional[Tuple[int, str]]:
        from .b_tree_index import BPlusTreeIndex

        if not os.path.exists(DEFAULT_BPLUS_INDEX_PATH):
            raise FileNotFoundError(
                f"B+ tree index file not found: {DEFAULT_BPLUS_INDEX_PATH}"
            )

        with BPlusTreeIndex() as idx:
            id_ = idx.query_by_hash(hash)
            if id_ is None:
                return None
            try:
                id_read, name, _ = self.read_record(id_)
            except Exception:
                return None
            return id_read, name

    def _command_update_record_linear(self, id_: int, new_name_str: str) -> int:
        self._validate_name(new_name_str, "new_name_str")

        with self._row_write_lock(id_):
            file_size = os.path.getsize(DEFAULT_DB_PATH)
            if id_ * RECORD_SIZE + RECORD_SIZE > file_size:
                raise IndexError("id out of range")

            record_offset = id_ * RECORD_SIZE
            new_name = new_name_str.encode("ascii")
            new_hash = compute_hash_for(id_, new_name)

            with open(DEFAULT_DB_PATH, "r+b") as f:
                f.seek(record_offset)
                old_record = f.read(RECORD_SIZE)
                _, old_name_b, _ = struct.unpack(RECORD_STRUCT, old_record)
                changed = old_name_b != new_name

                if not changed:
                    return 0

                packed = struct.pack(RECORD_STRUCT, id_, new_name, new_hash)
                f.seek(record_offset)
                f.write(packed)
                f.flush()

        return 1

    def _command_update_record_bplus(self, id_: int, new_name_str: str) -> int:
        self._validate_name(new_name_str, "new_name_str")

        if not os.path.exists(DEFAULT_BPLUS_INDEX_PATH):
            raise FileNotFoundError(
                f"B+ tree index file not found: {DEFAULT_BPLUS_INDEX_PATH}"
            )

        new_name = new_name_str.encode("ascii")
        new_hash = compute_hash_for(id_, new_name)

        with self._row_write_lock(id_):
            file_size = os.path.getsize(DEFAULT_DB_PATH)
            if id_ * RECORD_SIZE + RECORD_SIZE > file_size:
                raise IndexError("id out of range")

            record_offset = id_ * RECORD_SIZE
            with open(DEFAULT_DB_PATH, "r+b") as f:
                f.seek(record_offset)
                old_record = f.read(RECORD_SIZE)
                _, old_name_b, old_hash = struct.unpack(RECORD_STRUCT, old_record)
                changed = old_name_b != new_name

                if not changed:
                    return 0

                packed = struct.pack(RECORD_STRUCT, id_, new_name, new_hash)
                old_packed = struct.pack(RECORD_STRUCT, id_, old_name_b, old_hash)

                f.seek(record_offset)
                f.write(packed)
                f.flush()

                # Update the B+ tree index directly; BPlusTreeIndex handles concurrency.
                try:
                    with BPlusTreeIndex(writable=True) as idx:
                        success = idx.update(old_hash, new_hash, id_)
                        if not success:
                            # Roll back DB update when index update cannot be applied.
                            f.seek(record_offset)
                            f.write(old_packed)
                            f.flush()
                            return 0
                except Exception:
                    f.seek(record_offset)
                    f.write(old_packed)
                    f.flush()
                    raise

        return 1

    def update_record_with_bplus_index(self, id_: int, new_name_str: str) -> bool:
        """
        Update a record's name and sync the B-tree index.

        Returns True if update succeeded, False if index update failed.
        Updates both the database file and the B+ tree index.
        """
        return bool(self._command_update_record_bplus(id_, new_name_str))


def create_database(start: int = 0, end: int = 11_881_376) -> None:
    if start < 0:
        raise ValueError("start must be non-negative")
    if end < start:
        raise ValueError("end must be greater than or equal to start")

    db = DbEngine()

    def _print_progress(i: int, tot: int) -> None:
        bar_width = 30
        pct = (i / tot) * 100 if tot else 100.0
        filled = int((i / tot) * bar_width) if tot else bar_width
        bar = "#" * filled + "-" * (bar_width - filled)
        print(
            f"\r[{bar}] {pct:6.2f}% {i}/{tot}",
            end="" if i < tot else "\n",
            flush=True,
        )

    print(f"Ensuring capacity for {end} records...", flush=True)
    db.ensure_capacity(end)
    print(f"Populating records {start}..{end - 1}", flush=True)

    print_time(
        "Database build",
        lambda: db.populate_range(
            start,
            end,
            _print_progress,
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the emulator database.")
    parser.add_argument(
        "--start",
        type=int,
        default=0,
        help="First record id to populate.",
    )
    parser.add_argument(
        "--end",
        type=int,
        default=11_881_376,
        help="Stop populating before this record id.",
    )
    args = parser.parse_args()
    create_database(start=args.start, end=args.end)


if __name__ == "__main__":
    main()
