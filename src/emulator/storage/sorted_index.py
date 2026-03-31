import argparse
import heapq
import mmap
import os
import struct
import tempfile
import time
from typing import Iterator, List, Optional, Tuple

from .constants import (
    DB_RECORD_SIZE,
    DB_HASH_OFFSET,
    INDEX_STRUCT,
    INDEX_RECORD_SIZE,
    DEFAULT_INDEX_PATH,
    DEFAULT_DB_PATH,
)


class IndexBuilder:
    """
    Build a sorted on-disk index mapping sha256(hash) -> id.

    - Scans the source DB file in chunks to limit memory usage.
    - Creates sorted temporary chunk files and then merges them into final index file.
    """

    def __init__(self, chunk_size: int = 200_000):
        self.chunk_size = int(chunk_size)
        self.tmp_dir = os.path.join(os.path.dirname(DEFAULT_INDEX_PATH), "tmp")
        os.makedirs(self.tmp_dir, exist_ok=True)
        os.makedirs(os.path.dirname(DEFAULT_INDEX_PATH) or ".", exist_ok=True)

    def build(self) -> None:
        tmp_files: List[str] = []
        if not os.path.exists(DEFAULT_DB_PATH):
            raise FileNotFoundError(f"database file not found: {DEFAULT_DB_PATH}")

        total_records = os.path.getsize(DEFAULT_DB_PATH) // DB_RECORD_SIZE
        if total_records == 0:
            raise ValueError(
                "database is empty; build it first with `python -m emulator.storage.database`"
            )

        chunk_count = (total_records + self.chunk_size - 1) // self.chunk_size
        with open(DEFAULT_DB_PATH, "rb") as dbf:
            db_mm = mmap.mmap(dbf.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                for chunk_number, start in enumerate(
                    range(0, total_records, self.chunk_size), 1
                ):
                    end = min(total_records, start + self.chunk_size)
                    print(
                        f"Building chunk {chunk_number}/{chunk_count}: records {start}..{end - 1}",
                        flush=True,
                    )
                    entries: List[Tuple[bytes, int]] = []
                    for id_ in range(start, end):
                        off = id_ * DB_RECORD_SIZE
                        hb = db_mm[off + DB_HASH_OFFSET : off + DB_HASH_OFFSET + 32]
                        entries.append((hb, id_))
                    entries.sort(key=lambda x: x[0])
                    fd, tmp_path = tempfile.mkstemp(
                        prefix="idx_chunk_", dir=self.tmp_dir
                    )
                    os.close(fd)
                    with open(tmp_path, "wb") as tf:
                        for hb, id_ in entries:
                            tf.write(struct.pack(INDEX_STRUCT, hb, id_))
                    tmp_files.append(tmp_path)
                    del entries
            finally:
                db_mm.close()

        print(
            f"Merging {len(tmp_files)} chunk files into {DEFAULT_INDEX_PATH}",
            flush=True,
        )
        self._merge_chunks(tmp_files, DEFAULT_INDEX_PATH)

        for p in tmp_files:
            try:
                os.remove(p)
            except OSError:
                pass

    def _iter_chunk(self, path: str) -> Iterator[Tuple[bytes, int]]:
        with open(path, "rb") as f:
            while True:
                data = f.read(INDEX_RECORD_SIZE)
                if not data:
                    break
                hb, id_ = struct.unpack(INDEX_STRUCT, data)
                yield (hb, id_)

    def _merge_chunks(self, chunk_paths: List[str], out_path: str) -> None:
        iterators = [self._iter_chunk(p) for p in chunk_paths]
        with open(out_path, "wb") as out:
            for hb, id_ in heapq.merge(*iterators):
                out.write(struct.pack(INDEX_STRUCT, hb, id_))


class HashIndex:
    """
    Read-only sorted index supporting binary-search lookup by hash bytes.
    """

    def __init__(self):
        self._f = open(DEFAULT_INDEX_PATH, "rb")
        self._mm = mmap.mmap(self._f.fileno(), 0, access=mmap.ACCESS_READ)
        self._count = len(self._mm) // INDEX_RECORD_SIZE

    def close(self) -> None:
        try:
            self._mm.close()
        finally:
            try:
                self._f.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def query_by_hash(self, hash_bytes: bytes) -> Optional[int]:
        lo = 0
        hi = self._count - 1
        mm = self._mm

        while lo <= hi:
            mid = (lo + hi) // 2
            off = mid * INDEX_RECORD_SIZE
            mid_hash = mm[off : off + 32]
            if mid_hash == hash_bytes:
                _, id_ = struct.unpack_from(INDEX_STRUCT, mm, off)
                return int(id_)
            if mid_hash < hash_bytes:
                lo = mid + 1
            else:
                hi = mid - 1
        return None


def build_index(chunk_size: int = 200_000) -> None:
    start_time = time.perf_counter()
    builder = IndexBuilder(chunk_size=chunk_size)
    builder.build()
    elapsed = time.perf_counter() - start_time
    print(f"Index build completed in {elapsed:.2f} seconds", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the sorted hash index.")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=200_000,
        help="Number of records to sort per chunk before merging.",
    )
    args = parser.parse_args()
    build_index(chunk_size=args.chunk_size)


if __name__ == "__main__":
    main()
