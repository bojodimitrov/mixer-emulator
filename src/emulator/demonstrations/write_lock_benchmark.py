"""
Benchmark: row-level locking write throughput vs simulated serial writes.

Run with:
    python -m emulator.demonstrations.write_lock_benchmark

The benchmark fires N concurrent update_record calls against distinct rows using
an increasing number of worker threads.

- With the old table-level lock all workers serialized, so wall-clock time was
  roughly N_UPDATES * single_write_latency regardless of worker count.
- With row-level locking, workers targeting different rows run in parallel, so
  wall-clock time drops roughly proportional to worker count (up to I/O saturation).

A simulated serial baseline is also printed (1 worker = upper bound of old behaviour).
"""

import random
import string
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from emulator.storage.engine import DbEngine


def _random_name(seed: int) -> str:
    rng = random.Random(seed)
    return "".join(rng.choices(string.ascii_lowercase, k=5))


def _run_updates(db: DbEngine, ids: list, names: list, n_workers: int) -> float:
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = [
            pool.submit(db.update_record, id_, name) for id_, name in zip(ids, names)
        ]
        for fut in as_completed(futures):
            fut.result()
    return time.perf_counter() - start


def main() -> None:
    db = DbEngine()
    count = db.record_count()

    if count == 0:
        print("Database is empty. Run `python -m emulator.storage.database` first.")
        return

    n_updates = min(200, count)
    # Spread rows evenly across the file so every worker touches a different row.
    step = max(1, count // n_updates)
    ids = [i * step for i in range(n_updates)]

    # Save originals so we can restore them after each run.
    originals = {id_: db.read_record(id_)[1] for id_ in ids}
    new_names = [_random_name(i) for i in range(n_updates)]

    print(f"Database : {count:,} records")
    print(f"Updates  : {n_updates} (distinct rows, evenly spaced)")
    print()
    print(f"{'Workers':<10} {'Time (s)':<12} {'Updates/s':<14} {'Speedup vs 1'}")
    print("-" * 52)

    baseline: float | None = None

    for workers in [1, 2, 4, 8]:
        elapsed = _run_updates(db, ids, new_names, workers)

        if baseline is None:
            baseline = elapsed
            speedup = "—"
        else:
            speedup = f"{baseline / elapsed:.2f}x"

        rate = n_updates / elapsed
        print(f"{workers:<10} {elapsed:<12.3f} {rate:<14.0f} {speedup}")

        # Restore original values before the next run so each run starts clean.
        _run_updates(db, ids, list(originals.values()), workers)

    print()
    print("With row-level locking, speedup should scale with worker count.")
    print("(Capped by index serialisation and OS I/O scheduler.)")


if __name__ == "__main__":
    main()
