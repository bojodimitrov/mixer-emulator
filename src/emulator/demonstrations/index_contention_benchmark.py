"""Benchmark index contention during concurrent update writes.

Run examples:
    python -m emulator.demonstrations.index_contention_benchmark

This measures update throughput when all updates target distinct rows.
DB row writes can run concurrently while index updates contend on B+ tree paths.
"""

import argparse
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from emulator.storage.engine import DbEngine


def _random_name(seed: int) -> str:
    rng = random.Random(seed)
    return "".join(rng.choices(string.ascii_lowercase, k=5))


def _run_updates(db: DbEngine, ids: list[int], names: list[str], workers: int) -> float:
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(db.update_record, id_, name) for id_, name in zip(ids, names)
        ]
        for fut in as_completed(futures):
            fut.result()
    return time.perf_counter() - start


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark concurrent update contention for B+ index updates"
    )
    parser.add_argument(
        "--updates",
        type=int,
        default=120,
        help="Number of distinct rows to update per run",
    )
    parser.add_argument(
        "--workers",
        type=int,
        nargs="+",
        default=[1, 2, 4, 8],
        help="Thread counts to test",
    )
    args = parser.parse_args()

    db = DbEngine()
    count = db.record_count()
    if count == 0:
        print("Database is empty. Run python -m emulator.storage.database first.")
        return

    updates = min(args.updates, count)
    step = max(1, count // updates)
    ids = [i * step for i in range(updates)]

    originals = {id_: db.read_record(id_)[1] for id_ in ids}
    new_names = [_random_name(i) for i in range(updates)]

    print(f"Strategy : linear")
    print(f"Database : {count:,} records")
    print(f"Updates  : {updates} (distinct rows)")
    print()
    print(f"{'Workers':<8} {'Time (s)':<10} {'Upd/s':<8} {'Speedup':<8}")
    print("-" * 40)

    baseline: float | None = None

    for workers in args.workers:
        elapsed = _run_updates(db, ids, new_names, workers)
        if baseline is None:
            baseline = elapsed
            speedup = "-"
        else:
            speedup = f"{baseline / elapsed:.2f}x"

        rate = updates / elapsed
        print(f"{workers:<8} {elapsed:<10.3f} {rate:<8.0f} {speedup:<8}")

        # Restore for fair next run.
        restore_names = [originals[id_] for id_ in ids]
        _run_updates(db, ids, restore_names, workers)


if __name__ == "__main__":
    main()
