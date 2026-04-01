import argparse
import mmap
import os
import struct
from typing import Optional

from emulator.utils import print_time

from .constants import (
    INDEX_STRUCT,
    INDEX_RECORD_SIZE,
    DEFAULT_INDEX_PATH,
    DEFAULT_DB_PATH,
)
from .external_sort import build_sorted_hash_pairs


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
        build_sorted_hash_pairs(
            db_path=DEFAULT_DB_PATH,
            out_path=DEFAULT_INDEX_PATH,
            tmp_dir=self.tmp_dir,
            chunk_size=self.chunk_size,
            chunk_prefix="Building chunks",
            merge_prefix="Merging index",
            tmp_prefix="idx_chunk_",
            merge_message=f"Merging chunk files into {DEFAULT_INDEX_PATH}",
        )


class HashIndex:
    """
    Sorted index supporting binary-search lookup by hash bytes.

    - Read mode (default): query only.
    - Writable mode: supports O(n) insert/delete by shifting file contents.
    """

    def __init__(self, writable: bool = False):
        self._writable = writable
        mode = "r+b" if writable else "rb"
        self._f = open(DEFAULT_INDEX_PATH, mode)
        self._mm = None
        self._count = 0
        self._refresh_mapping()

    def _refresh_mapping(self) -> None:
        if self._mm is not None:
            self._mm.close()
            self._mm = None

        size = os.path.getsize(DEFAULT_INDEX_PATH)
        self._count = size // INDEX_RECORD_SIZE
        if size == 0:
            return

        access = mmap.ACCESS_WRITE if self._writable else mmap.ACCESS_READ
        self._mm = mmap.mmap(self._f.fileno(), 0, access=access)

    @staticmethod
    def _ensure_hash_size(hash_bytes: bytes) -> None:
        if len(hash_bytes) != 32:
            raise ValueError("hash_bytes must be exactly 32 bytes")

    def _ensure_writable(self) -> None:
        if not self._writable:
            raise RuntimeError("HashIndex must be opened with writable=True")

    def _hash_at(self, index: int) -> bytes:
        if self._mm is None:
            raise IndexError("index file is empty")
        off = index * INDEX_RECORD_SIZE
        return self._mm[off : off + 32]

    def _bisect_left(self, hash_bytes: bytes) -> int:
        lo = 0
        hi = self._count
        while lo < hi:
            mid = (lo + hi) // 2
            if self._hash_at(mid) < hash_bytes:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def _shift_right(self, offset: int, length: int) -> None:
        if length <= 0:
            return
        chunk_size = 4 * 1024 * 1024
        remaining = length
        while remaining > 0:
            chunk = min(chunk_size, remaining)
            read_pos = offset + remaining - chunk
            self._f.seek(read_pos)
            data = self._f.read(chunk)
            self._f.seek(read_pos + INDEX_RECORD_SIZE)
            self._f.write(data)
            remaining -= chunk

    def _shift_left(self, offset: int, length: int) -> None:
        if length <= 0:
            return
        chunk_size = 4 * 1024 * 1024
        moved = 0
        while moved < length:
            chunk = min(chunk_size, length - moved)
            read_pos = offset + INDEX_RECORD_SIZE + moved
            self._f.seek(read_pos)
            data = self._f.read(chunk)
            self._f.seek(offset + moved)
            self._f.write(data)
            moved += chunk

    def close(self) -> None:
        try:
            if self._mm is not None:
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

    def query_by_hash(self, hash: str) -> Optional[int]:
        hash_bytes = bytes.fromhex(hash)

        self._ensure_hash_size(hash_bytes)
        if self._count == 0 or self._mm is None:
            return None

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

    def insert(self, hash_bytes: bytes, id_: int) -> None:
        self._ensure_writable()
        self._ensure_hash_size(hash_bytes)

        insert_at = self._bisect_left(hash_bytes)
        if insert_at < self._count and self._hash_at(insert_at) == hash_bytes:
            raise ValueError("hash already exists in sorted index")

        old_size = self._count * INDEX_RECORD_SIZE
        off = insert_at * INDEX_RECORD_SIZE

        if self._mm is not None:
            self._mm.close()
            self._mm = None

        self._f.truncate(old_size + INDEX_RECORD_SIZE)
        self._shift_right(off, old_size - off)
        self._f.seek(off)
        self._f.write(struct.pack(INDEX_STRUCT, hash_bytes, id_))
        self._f.flush()
        self._refresh_mapping()

    def delete(self, hash_bytes: bytes) -> bool:
        self._ensure_writable()
        self._ensure_hash_size(hash_bytes)

        if self._count == 0:
            return False

        delete_at = self._bisect_left(hash_bytes)
        if delete_at >= self._count or self._hash_at(delete_at) != hash_bytes:
            return False

        old_size = self._count * INDEX_RECORD_SIZE
        off = delete_at * INDEX_RECORD_SIZE

        if self._mm is not None:
            self._mm.close()
            self._mm = None

        self._shift_left(off, old_size - (off + INDEX_RECORD_SIZE))
        self._f.truncate(old_size - INDEX_RECORD_SIZE)
        self._f.flush()
        self._refresh_mapping()
        return True


def build_index(chunk_size: int = 200_000) -> None:
    builder = IndexBuilder(chunk_size=chunk_size)
    print_time("Sorted index build", lambda: builder.build())


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
