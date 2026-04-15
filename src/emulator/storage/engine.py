import argparse
import concurrent.futures
import errno
import threading
from contextlib import contextmanager
import mmap
import os
import struct
import time
from typing import Callable, Dict, Optional, Tuple

from emulator.transport_layer.transport import hex_from_bytes

from ..utils import compute_hash_for, id_to_name, print_time
from .constants import (
    RECORD_STRUCT,
    RECORD_SIZE,
    DB_HASH_OFFSET,
    DEFAULT_DB_PATH,
    DB_RECORD_SIZE,
    SHARD_COUNT,
    db_shard_path,
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
        db_path: Optional[str] = None,
        shard_index: int = 0,
        shard_count: int = 1,
    ):
        if shard_count < 1:
            raise ValueError("shard_count must be >= 1")
        if not (0 <= shard_index < shard_count):
            raise ValueError("shard_index must be in [0, shard_count)")

        self._db_path = db_path or DEFAULT_DB_PATH
        self._shard_index = shard_index
        self._shard_count = shard_count
        os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
        open(self._db_path, "a").close()
        self._lock_path = f"{self._db_path}.lock"
        open(self._lock_path, "a").close()

    def _local_id(self, global_id: int) -> int:
        """Map a global record id to its local (file-offset) position in this shard."""
        return global_id // self._shard_count

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
        return os.path.getsize(self._db_path) // RECORD_SIZE

    def ensure_capacity(self, capacity: int) -> None:
        # Number of records owned by this shard: those with global_id % shard_count == shard_index
        n = max(0, capacity - self._shard_index)
        shard_local_cap = (n + self._shard_count - 1) // self._shard_count
        target = shard_local_cap * RECORD_SIZE
        with self._write_lock():
            with open(self._db_path, "ab") as f:
                f.tell()

            cur = os.path.getsize(self._db_path)
            if cur < target:
                with open(self._db_path, "r+b") as f:
                    f.truncate(target)

    def populate_range(self, start: int, end: int, progress_callback: Optional[callable] = None) -> None:  # type: ignore
        shard_ids = [
            id_ for id_ in range(start, end)
            if id_ % self._shard_count == self._shard_index
        ]
        total = len(shard_ids)
        with self._write_lock():
            with open(self._db_path, "r+b") as f:
                mm = mmap.mmap(f.fileno(), 0)
                try:
                    for i, id_ in enumerate(shard_ids, 1):
                        name = id_to_name(id_)
                        hashb = compute_hash_for(id_, name)
                        packed = struct.pack(RECORD_STRUCT, id_, name, hashb)
                        off = self._local_id(id_) * RECORD_SIZE
                        mm[off : off + RECORD_SIZE] = packed
                        if progress_callback and (i % 10000 == 0 or i == total):
                            progress_callback(i, total)
                finally:
                    mm.close()

    def read_record(self, id_: int) -> Tuple[int, str, str]:
        local_id = self._local_id(id_)
        with self._row_read_lock(id_):
            file_size = os.path.getsize(self._db_path)
            if local_id * RECORD_SIZE + RECORD_SIZE > file_size:
                raise IndexError("id out of range")
            with open(self._db_path, "rb") as f:
                f.seek(local_id * RECORD_SIZE)
                data = f.read(RECORD_SIZE)

        id_read, name_b, hashb = struct.unpack(RECORD_STRUCT, data)
        return id_read, name_b.decode("ascii"), hex_from_bytes(hashb)

    def get_by_id(self, global_id: int) -> Optional[Tuple[int, str]]:
        """Read a record directly by its global id. Returns (id, name) or None."""
        local_id = self._local_id(global_id)
        with self._row_read_lock(global_id):
            file_size = os.path.getsize(self._db_path)
            if local_id * RECORD_SIZE + RECORD_SIZE > file_size:
                return None
            with open(self._db_path, "rb") as f:
                f.seek(local_id * RECORD_SIZE)
                data = f.read(RECORD_SIZE)
        id_read, name_b, _ = struct.unpack(RECORD_STRUCT, data)
        return int(id_read), name_b.decode("ascii")

    def count_corrupted_records(
        self,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Count rows where stored name differs from canonical id_to_name(id)."""
        # No file lock needed: DbServer owns the file; slight staleness is
        # acceptable for metrics scans.
        file_size = os.path.getsize(self._db_path)

        if file_size == 0:
            if progress_callback is not None:
                progress_callback(0, 0)
            return 0

        corrupted = 0
        with open(self._db_path, "rb") as f:
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
        return self._query_by_hash_linear(hash)

    def update_record(self, id_: int, new_name_str: str) -> bool:
        return bool(self._command_update_record_linear(id_, new_name_str))

    def command_update_record(self, id_: int, new_name_str: str) -> int:
        return self._command_update_record_linear(id_, new_name_str)

    def _query_by_hash_linear(self, hash: str) -> Optional[Tuple[int, str]]:
        hash_bytes = bytes.fromhex(hash)

        # No file lock: DbServer owns the file; concurrent writes are serialised
        # per-row by stripe locks, making a file-wide LOCK_SH unnecessary.
        file_size = os.path.getsize(self._db_path)

        if file_size == 0:
            return None

        with open(self._db_path, "rb") as f:
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

    def _command_update_record_linear(self, id_: int, new_name_str: str) -> int:
        self._validate_name(new_name_str, "new_name_str")
        local_id = self._local_id(id_)
        with self._row_write_lock(id_):
            file_size = os.path.getsize(self._db_path)
            if local_id * RECORD_SIZE + RECORD_SIZE > file_size:
                raise IndexError("id out of range")

            record_offset = local_id * RECORD_SIZE
            new_name = new_name_str.encode("ascii")
            new_hash = compute_hash_for(id_, new_name)

            with open(self._db_path, "r+b") as f:
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

    def command_update_record_with_hashes(
        self, id_: int, new_name_str: str
    ) -> Tuple[int, str, str]:
        """Update record; return (updated_int, old_hash_hex, new_hash_hex).

        Used by shard servers so the caller (microservice) can propagate the
        change to the GSI without the DB engine touching the index directly.
        """
        self._validate_name(new_name_str, "new_name_str")
        local_id = self._local_id(id_)

        with self._row_write_lock(id_):
            file_size = os.path.getsize(self._db_path)
            if local_id * RECORD_SIZE + RECORD_SIZE > file_size:
                raise IndexError("id out of range")

            record_offset = local_id * RECORD_SIZE
            new_name = new_name_str.encode("ascii")
            new_hash = compute_hash_for(id_, new_name)

            with open(self._db_path, "r+b") as f:
                f.seek(record_offset)
                old_record = f.read(RECORD_SIZE)
                _, old_name_b, old_hash_b = struct.unpack(RECORD_STRUCT, old_record)
                changed = old_name_b != new_name

                if not changed:
                    return 0, hex_from_bytes(old_hash_b), hex_from_bytes(old_hash_b)

                packed = struct.pack(RECORD_STRUCT, id_, new_name, new_hash)
                f.seek(record_offset)
                f.write(packed)
                f.flush()

        return 1, hex_from_bytes(old_hash_b), hex_from_bytes(new_hash)

def create_database(start: int = 0, end: int = 11_881_376) -> None:
    """Build all SHARD_COUNT shard DB files in parallel.

    Each shard i receives records where ``global_id % SHARD_COUNT == i``.
    The shard files are written to the paths returned by ``db_shard_path(i)``.
    After this runs, build the GSI with ``python -m emulator.storage.b_tree_index``.
    """
    if start < 0:
        raise ValueError("start must be non-negative")
    if end < start:
        raise ValueError("end must be greater than or equal to start")

    lock = threading.Lock()
    counters = [0] * SHARD_COUNT

    def _make_progress(shard_i: int):
        def _cb(done: int, total: int) -> None:
            with lock:
                counters[shard_i] = done
                overall = sum(counters)
                total_all = end - start
                pct = overall / total_all * 100 if total_all else 100.0
                bar_width = 30
                filled = int(pct / 100 * bar_width)
                bar = "#" * filled + "-" * (bar_width - filled)
                print(
                    f"\r[{bar}] {pct:5.1f}%  (shard {shard_i}: {done}/{total})",
                    end="",
                    flush=True,
                )
        return _cb

    def _build_shard(i: int) -> None:
        path = db_shard_path(i)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        db = DbEngine(db_path=path, shard_index=i, shard_count=SHARD_COUNT)
        db.ensure_capacity(end)
        db.populate_range(start, end, _make_progress(i))

    print(f"Building {SHARD_COUNT} shards for records [{start}, {end})...", flush=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=SHARD_COUNT) as pool:
        futures = [pool.submit(_build_shard, i) for i in range(SHARD_COUNT)]
        for f in concurrent.futures.as_completed(futures):
            f.result()  # re-raise any exception
    print(flush=True)  # newline after progress bar
    print(f"Done. Build GSI next: python -m emulator.storage.b_tree_index", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            f"Build the {SHARD_COUNT} sharded DB files. "
            "After this, run `python -m emulator.storage.b_tree_index` to build the GSI."
        )
    )
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
