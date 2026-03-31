import random
import time

from .storage.database import FileDB


def _measure_lookup(db: FileDB, strategy_name: str, hash_bytes: bytes) -> None:
    t0 = time.perf_counter()
    result = db.query_by_hash(hash_bytes)
    t1 = time.perf_counter()
    print(f"{strategy_name:>6} -> {result}")
    print(f"{strategy_name:>6} lookup took {(t1 - t0) * 1000:.3f} ms")
    print()


def run_app():
    linear_db = FileDB(lookup_strategy=FileDB.STRATEGY_LINEAR)
    sorted_db = FileDB(lookup_strategy=FileDB.STRATEGY_SORTED)
    bplus_db = FileDB(lookup_strategy=FileDB.STRATEGY_BPLUS)

    record_count = linear_db.record_count()
    if record_count == 0:
        raise RuntimeError(
            "database is empty; build it first with `python -m emulator.storage.database`"
        )

    record_id = random.randint(0, record_count - 1)

    t0 = time.perf_counter()
    id_read, name, hashb = linear_db.read_record(record_id)
    t1 = time.perf_counter()
    print(f"Read record {record_id} in {(t1 - t0) * 1000:.3f} ms")
    print(f"id={id_read} name={name} hash={hashb.hex()}")

    _measure_lookup(linear_db, "linear", hashb)
    _measure_lookup(sorted_db, "sorted", hashb)
    _measure_lookup(bplus_db, "bplus", hashb)


if __name__ == "__main__":
    run_app()
