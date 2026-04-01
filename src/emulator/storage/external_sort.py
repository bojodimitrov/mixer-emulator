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
    db_path: str,
    out_path: str,
    tmp_dir: str,
    chunk_size: int,
    *,
    chunk_prefix: str,
    merge_prefix: str,
    tmp_prefix: str,
    merge_message: Optional[str] = None,
) -> int:
    """External-sort (hash, id) pairs from DB records into a sorted binary file."""
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"database file not found: {db_path}")

    os.makedirs(tmp_dir, exist_ok=True)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    total_records = os.path.getsize(db_path) // DB_RECORD_SIZE
    if total_records == 0:
        raise ValueError(
            "database is empty; build it first with `python -m emulator.storage.database`"
        )

    tmp_files: List[str] = []
    chunk_count = (total_records + chunk_size - 1) // chunk_size

    with open(db_path, "rb") as dbf:
        db_mm = mmap.mmap(dbf.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            for chunk_number, start in enumerate(
                range(0, total_records, chunk_size), 1
            ):
                end = min(total_records, start + chunk_size)
                _print_progress(
                    chunk_prefix,
                    chunk_number,
                    chunk_count,
                    suffix=f"records {start}..{end - 1}",
                )

                entries: List[Tuple[bytes, int]] = []
                for id_ in range(start, end):
                    off = id_ * DB_RECORD_SIZE
                    hb = db_mm[off + DB_HASH_OFFSET : off + DB_HASH_OFFSET + 32]
                    entries.append((hb, id_))

                entries.sort(key=lambda x: x[0])
                fd, tmp_path = tempfile.mkstemp(prefix=tmp_prefix, dir=tmp_dir)
                os.close(fd)
                with open(tmp_path, "wb") as tf:
                    for hb, id_ in entries:
                        tf.write(struct.pack(INDEX_STRUCT, hb, id_))
                tmp_files.append(tmp_path)
        finally:
            db_mm.close()

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
