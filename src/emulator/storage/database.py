import argparse
import hashlib
import mmap
import os
import struct
import time
from typing import Literal, Optional, Tuple

from .constants import (
    RECORD_STRUCT,
    RECORD_SIZE,
    DB_HASH_OFFSET,
    DEFAULT_BPLUS_INDEX_PATH,
    DEFAULT_DB_PATH,
    DB_RECORD_SIZE,
    DEFAULT_INDEX_PATH,
)


class FileDB:
    STRATEGY_LINEAR = "linear"
    STRATEGY_SORTED = "sorted"
    STRATEGY_BPLUS = "bplus"
    LOOKUP_STRATEGIES = {
        STRATEGY_LINEAR,
        STRATEGY_SORTED,
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
        lookup_strategy: Literal["linear", "sorted", "bplus"] = STRATEGY_LINEAR,
    ):
        if lookup_strategy not in self.LOOKUP_STRATEGIES:
            supported = ", ".join(sorted(self.LOOKUP_STRATEGIES))
            raise ValueError(
                f"unsupported lookup_strategy={lookup_strategy!r}; expected one of: {supported}"
            )

        self.lookup_strategy = lookup_strategy
        os.makedirs(os.path.dirname(DEFAULT_DB_PATH) or ".", exist_ok=True)
        open(DEFAULT_DB_PATH, "a").close()

    @staticmethod
    def _id_to_name(idx: int) -> bytes:
        result = bytearray(5)
        n = idx
        for i in range(4, -1, -1):
            result[i] = ord("a") + (n % 26)
            n //= 26
        return bytes(result)

    @staticmethod
    def compute_hash_for(id_: int, name: bytes) -> bytes:
        s = f"{id_}:{name.decode('ascii')}".encode("utf-8")
        return hashlib.sha256(s).digest()

    def record_count(self) -> int:
        size = os.path.getsize(DEFAULT_DB_PATH)
        return size // RECORD_SIZE

    def ensure_capacity(self, capacity: int) -> None:
        target = capacity * RECORD_SIZE

        with open(DEFAULT_DB_PATH, "ab") as f:
            f.tell()

        cur = os.path.getsize(DEFAULT_DB_PATH)
        if cur < target:
            with open(DEFAULT_DB_PATH, "r+b") as f:
                f.truncate(target)

    def update_record(self, id_: int, name_str: str) -> None:
        if (
            not isinstance(name_str, str)
            or len(name_str) != 5
            or not name_str.isalpha()
            or not name_str.islower()
        ):
            raise ValueError("name_str must be a 5-letter lowercase ASCII word")

        file_size = os.path.getsize(DEFAULT_DB_PATH)
        if id_ * RECORD_SIZE + RECORD_SIZE > file_size:
            raise IndexError("id out of range")

        name = name_str.encode("ascii")
        hashb = self.compute_hash_for(id_, name)
        packed = struct.pack(RECORD_STRUCT, id_, name, hashb)

        with open(DEFAULT_DB_PATH, "r+b") as f:
            f.seek(id_ * RECORD_SIZE)
            f.write(packed)
            f.flush()

    def populate_range(self, start: int, end: int, progress_callback: Optional[callable] = None) -> None:  # type: ignore
        total = end - start
        with open(DEFAULT_DB_PATH, "r+b") as f:
            mm = mmap.mmap(f.fileno(), 0)
            try:
                for i, id_ in enumerate(range(start, end), 1):
                    name = self._id_to_name(id_)
                    hashb = self.compute_hash_for(id_, name)
                    packed = struct.pack(RECORD_STRUCT, id_, name, hashb)
                    off = id_ * RECORD_SIZE
                    mm[off : off + RECORD_SIZE] = packed
                    if progress_callback and (i % 10000 == 0 or i == total):
                        progress_callback(i, total)
            finally:
                mm.close()

    def read_record(self, id_: int) -> Tuple[int, str, bytes]:
        file_size = os.path.getsize(DEFAULT_DB_PATH)
        if id_ * RECORD_SIZE + RECORD_SIZE > file_size:
            raise IndexError("id out of range")

        with open(DEFAULT_DB_PATH, "rb") as f:
            f.seek(id_ * RECORD_SIZE)
            data = f.read(RECORD_SIZE)

        id_read, name_b, hashb = struct.unpack(RECORD_STRUCT, data)
        return id_read, name_b.decode("ascii"), hashb

    def query_by_hash(self, hash_bytes: bytes) -> Optional[Tuple[int, str]]:
        if self.lookup_strategy == self.STRATEGY_LINEAR:
            return self._query_by_hash_linear(hash_bytes)
        if self.lookup_strategy == self.STRATEGY_SORTED:
            return self._query_by_hash_sorted(hash_bytes)
        return self._query_by_hash_bplus(hash_bytes)

    def _query_by_hash_linear(self, hash_bytes: bytes) -> Optional[Tuple[int, str]]:
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

    def _query_by_hash_sorted(self, hash_bytes: bytes) -> Optional[Tuple[int, str]]:
        from .sorted_index import HashIndex

        if not os.path.exists(DEFAULT_INDEX_PATH):
            raise FileNotFoundError(f"index file not found: {DEFAULT_INDEX_PATH}")

        with HashIndex() as idx:
            id_ = idx.query_by_hash(hash_bytes)
            if id_ is None:
                return None
            try:
                id_read, name, _ = self.read_record(id_)
            except Exception:
                return None
            return id_read, name

    def _query_by_hash_bplus(self, hash_bytes: bytes) -> Optional[Tuple[int, str]]:
        from .b_tree_index import BPlusTreeIndex

        if not os.path.exists(DEFAULT_BPLUS_INDEX_PATH):
            raise FileNotFoundError(
                f"B+ tree index file not found: {DEFAULT_BPLUS_INDEX_PATH}"
            )

        with BPlusTreeIndex() as idx:
            id_ = idx.query_by_hash(hash_bytes)
            if id_ is None:
                return None
            try:
                id_read, name, _ = self.read_record(id_)
            except Exception:
                return None
            return id_read, name

    def query_by_hash_index(self, hash_bytes: bytes) -> Optional[Tuple[int, str]]:
        return self._query_by_hash_sorted(hash_bytes)

    def query_by_hash_bplus(self, hash_bytes: bytes) -> Optional[Tuple[int, str]]:
        return self._query_by_hash_bplus(hash_bytes)

    def update_record_with_bplus_index(self, id_: int, new_name_str: str) -> bool:
        """
        Update a record's name and sync the B-tree index.

        Returns True if update succeeded, False if index update failed.
        Updates both the database file and the B+ tree index.
        """
        if (
            not isinstance(new_name_str, str)
            or len(new_name_str) != 5
            or not new_name_str.isalpha()
            or not new_name_str.islower()
        ):
            raise ValueError("new_name_str must be a 5-letter lowercase ASCII word")

        if not os.path.exists(DEFAULT_BPLUS_INDEX_PATH):
            raise FileNotFoundError(
                f"B+ tree index file not found: {DEFAULT_BPLUS_INDEX_PATH}"
            )

        # Read the current record to get old hash
        try:
            _, old_name, old_hash = self.read_record(id_)
        except IndexError:
            raise IndexError("id out of range")

        # Compute new hash
        new_name = new_name_str.encode("ascii")
        new_hash = self.compute_hash_for(id_, new_name)

        # Update the database file
        packed = struct.pack(RECORD_STRUCT, id_, new_name, new_hash)
        with open(DEFAULT_DB_PATH, "r+b") as f:
            f.seek(id_ * RECORD_SIZE)
            f.write(packed)
            f.flush()

        # Update the B-tree index
        from .b_tree_index import BPlusTreeIndex

        try:
            with BPlusTreeIndex(writable=True) as idx:
                success = idx.update(old_hash, new_hash, id_)
                if not success:
                    # Rollback database update on index update failure
                    old_packed = struct.pack(
                        RECORD_STRUCT, id_, old_name.encode("ascii"), old_hash
                    )
                    with open(DEFAULT_DB_PATH, "r+b") as f:
                        f.seek(id_ * RECORD_SIZE)
                        f.write(old_packed)
                        f.flush()
                    return False
        except RuntimeError as e:
            # Rollback database update if B-tree operation fails
            old_packed = struct.pack(
                RECORD_STRUCT, id_, old_name.encode("ascii"), old_hash
            )
            with open(DEFAULT_DB_PATH, "r+b") as f:
                f.seek(id_ * RECORD_SIZE)
                f.write(old_packed)
                f.flush()
            raise e

        return True


def create_database(start: int = 0, end: int = 11_881_376) -> None:
    if start < 0:
        raise ValueError("start must be non-negative")
    if end < start:
        raise ValueError("end must be greater than or equal to start")

    db = FileDB()
    print(f"Ensuring capacity for {end} records...", flush=True)
    db.ensure_capacity(end)
    print(f"Populating records {start}..{end - 1}", flush=True)
    start_time = time.perf_counter()
    db.populate_range(
        start,
        end,
        lambda i, tot: print(f"{i}/{tot} ({(i / tot) * 100:.2f}%)", flush=True),
    )
    elapsed = time.perf_counter() - start_time
    print(f"Completed in {elapsed:.2f} seconds", flush=True)


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
