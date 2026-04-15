import heapq
import mmap
import os
import struct
import tempfile
from typing import Iterator, List, Optional, Tuple

from .constants import (
    DB_HASH_OFFSET,
    DB_RECORD_SIZE,
    INDEX_RECORD_SIZE,
    INDEX_STRUCT,
)


def _print_progress(prefix: str, current: int, total: int, suffix: str = "") -> None:
    bar_width = 30
    pct = (current / total) * 100 if total else 100.0
    filled = int((current / total) * bar_width) if total else bar_width
    bar = "#" * filled + "-" * (bar_width - filled)
    text = f"\r{prefix} [{bar}] {pct:6.2f}% {current}/{total}"
    if suffix:
        text += f" | {suffix}"
    print(text, end="" if current < total else "\n", flush=True)


def _iter_chunk(path: str) -> Iterator[Tuple[bytes, int]]:
    with open(path, "rb") as f:
        while True:
            data = f.read(INDEX_RECORD_SIZE)
            if not data:
                break
            hb, id_ = struct.unpack(INDEX_STRUCT, data)
            yield (hb, id_)


def build_sorted_hash_pairs(
    db_paths: List[str],
    out_path: str,
    tmp_dir: str,
    chunk_size: int,
    *,
    chunk_prefix: str,
    merge_prefix: str,
    tmp_prefix: str,
    merge_message: Optional[str] = None,
) -> int:
    """External-sort (hash, global_id) pairs from DB records into a sorted binary file.

    ``db_paths`` is a list of DB files to aggregate — typically the 4 shard files,
    but also accepts a single-element list for a flat (non-sharded) DB.
    The global_id is read directly from each stored record (not inferred from row offset),
    so shard files produce the correct global IDs without any shard-specific logic here.
    """
    for path in db_paths:
        if not os.path.exists(path):
            raise FileNotFoundError(f"database file not found: {path}")

    os.makedirs(tmp_dir, exist_ok=True)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    total_records = sum(os.path.getsize(p) // DB_RECORD_SIZE for p in db_paths)
    if total_records == 0:
        raise ValueError(
            "database is empty; build it first with `python -m emulator.storage.database --sharded`"
        )

    tmp_files: List[str] = []
    chunk_count = (total_records + chunk_size - 1) // chunk_size
    chunk_number = 0
    entries: List[Tuple[bytes, int]] = []
    processed = 0

    def _flush_chunk() -> None:
        nonlocal entries, chunk_number
        entries.sort(key=lambda x: x[0])
        fd, tmp_path = tempfile.mkstemp(prefix=tmp_prefix, dir=tmp_dir)
        os.close(fd)
        with open(tmp_path, "wb") as tf:
            for hb, gid in entries:
                tf.write(struct.pack(INDEX_STRUCT, hb, gid))
        tmp_files.append(tmp_path)
        entries = []
        chunk_number += 1

    for path in db_paths:
        file_records = os.path.getsize(path) // DB_RECORD_SIZE
        if file_records == 0:
            continue
        with open(path, "rb") as dbf:
            db_mm = mmap.mmap(dbf.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                for local_id in range(file_records):
                    off = local_id * DB_RECORD_SIZE
                    # Read global_id directly from the stored record (first 8 bytes).
                    global_id = struct.unpack_from("<Q", db_mm, off)[0]
                    hb = bytes(db_mm[off + DB_HASH_OFFSET : off + DB_HASH_OFFSET + 32])
                    entries.append((hb, global_id))
                    processed += 1
                    if len(entries) >= chunk_size:
                        _print_progress(
                            chunk_prefix,
                            chunk_number + 1,
                            chunk_count,
                            suffix=f"records ..{processed - 1}",
                        )
                        _flush_chunk()
            finally:
                db_mm.close()

    if entries:
        _print_progress(
            chunk_prefix,
            chunk_number + 1,
            chunk_count,
            suffix=f"records ..{processed - 1}",
        )
        _flush_chunk()

    message = merge_message or f"Merging {len(tmp_files)} chunk files"
    print(message, flush=True)

    iterators = [_iter_chunk(p) for p in tmp_files]
    with open(out_path, "wb") as out:
        merged = 0
        for hb, id_ in heapq.merge(*iterators):
            out.write(struct.pack(INDEX_STRUCT, hb, id_))
            merged += 1
            if merged % 10000 == 0 or merged == total_records:
                _print_progress(merge_prefix, merged, total_records)

    for p in tmp_files:
        try:
            os.remove(p)
        except OSError:
            pass

    return total_records
