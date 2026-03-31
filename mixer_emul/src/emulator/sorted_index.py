import os
import struct
import mmap
import heapq
import tempfile
from typing import List, Tuple, Optional, Iterator
from .constants import DB_RECORD_SIZE, DB_HASH_OFFSET, INDEX_STRUCT, INDEX_RECORD_SIZE, DEFAULT_INDEX_PATH, DEFAULT_DB_PATH


class IndexBuilder:
    """
    Build a sorted on-disk index mapping sha256(hash) -> id.

    - Scans the source DB file in chunks to limit memory usage.
    - Creates sorted temporary chunk files and then merges them into final index file.
    """
    def __init__(self, chunk_size: int = 200_000):
        self.tmp_dir = "./tmp"
        self.chunk_size = int(chunk_size)
        os.makedirs(self.tmp_dir, exist_ok=True)
        os.makedirs(os.path.dirname(DEFAULT_INDEX_PATH) or ".", exist_ok=True)

    def build(self) -> None:
        tmp_files: List[str] = []
        total_records = os.path.getsize(DEFAULT_DB_PATH) // DB_RECORD_SIZE
        with open(DEFAULT_DB_PATH, "rb") as dbf:
            db_mm = mmap.mmap(dbf.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                for start in range(0, total_records, self.chunk_size):
                    end = min(total_records, start + self.chunk_size)
                    entries: List[Tuple[bytes, int]] = []
                    for id_ in range(start, end):
                        off = id_ * DB_RECORD_SIZE
                        hb = db_mm[off + DB_HASH_OFFSET : off + DB_HASH_OFFSET + 32]
                        entries.append((hb, id_))
                    # sort chunk by hash
                    entries.sort(key=lambda x: x[0])
                    # write chunk to temp file
                    fd, tmp_path = tempfile.mkstemp(prefix="idx_chunk_", dir=self.tmp_dir)
                    os.close(fd)
                    with open(tmp_path, "wb") as tf:
                        for hb, id_ in entries:
                            tf.write(struct.pack(INDEX_STRUCT, hb, id_))
                    tmp_files.append(tmp_path)
                    # allow GC
                    del entries
            finally:
                db_mm.close()

        # merge sorted chunk files into final index
        self._merge_chunks(tmp_files, DEFAULT_INDEX_PATH)

        # remove temp files
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
        # heapq.merge merges sorted iterables; tuples compare lexicographically (hash, id)
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
        """Return id for exact matching hash_bytes or None."""
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
            elif mid_hash < hash_bytes:
                lo = mid + 1
            else:
                hi = mid - 1
        return None
