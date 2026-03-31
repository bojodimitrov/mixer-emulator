import random
import time
from typing import Tuple

from ..storage.database import FileDB
from ..storage.server import DatabaseRequest, DatabaseServer
from ..utils import compute_hash_for


def _measure_lookup(
    server: DatabaseServer, strategy_name: str, hash_bytes: bytes
) -> None:
    t0 = time.perf_counter()
    result = server.handle_request(DatabaseRequest("Query", {"hash_bytes": hash_bytes}))
    t1 = time.perf_counter()
    print(f"{strategy_name:>6} -> {result}")
    print(f"{strategy_name:>6} lookup took {(t1 - t0) * 1000:.3f} ms")
    print()


def create_servers() -> Tuple[DatabaseServer, DatabaseServer, DatabaseServer]:
    return (
        DatabaseServer(lookup_strategy=FileDB.STRATEGY_LINEAR),
        DatabaseServer(lookup_strategy=FileDB.STRATEGY_SORTED),
        DatabaseServer(lookup_strategy=FileDB.STRATEGY_BPLUS),
    )


def run_lookup_demo(
    linear_db: FileDB,
    linear_server: DatabaseServer,
    sorted_server: DatabaseServer,
    bplus_server: DatabaseServer,
) -> None:
    record_count = linear_db.record_count()
    record_id = random.randint(0, record_count - 1)

    t0 = time.perf_counter()
    id_read, name, hashb = linear_db.read_record(record_id)
    t1 = time.perf_counter()
    print(f"Read record {record_id} in {(t1 - t0) * 1000:.3f} ms")
    print(f"id={id_read} name={name} hash={hashb.hex()}")

    _measure_lookup(linear_server, "linear", hashb)
    _measure_lookup(sorted_server, "sorted", hashb)
    _measure_lookup(bplus_server, "bplus", hashb)


def run_update_demo(linear_db: FileDB, bplus_server: DatabaseServer) -> None:
    record_count = linear_db.record_count()

    update_id = random.randint(0, record_count - 1)
    _, old_update_name, old_update_hash = linear_db.read_record(update_id)
    updated_name = "zzzzz" if old_update_name != "zzzzz" else "yyyyy"

    t0 = time.perf_counter()
    updated = bplus_server.handle_request(
        DatabaseRequest("Command", {"id": update_id, "new_name": updated_name})
    )
    t1 = time.perf_counter()
    print(f"bplus update id={update_id} -> {updated}")
    print(f"bplus update took {(t1 - t0) * 1000:.3f} ms")

    updated_result = bplus_server.handle_request(
        DatabaseRequest(
            "Query",
            {"hash_bytes": compute_hash_for(update_id, updated_name.encode("ascii"))},
        )
    )
    old_result = bplus_server.handle_request(
        DatabaseRequest("Query", {"hash_bytes": old_update_hash})
    )
    print(f"bplus verify new hash -> {updated_result}")
    print(f"bplus verify old hash -> {old_result}")


def run_database_demo() -> None:
    linear_db = FileDB(lookup_strategy=FileDB.STRATEGY_LINEAR)
    linear_server, sorted_server, bplus_server = create_servers()

    run_lookup_demo(linear_db, linear_server, sorted_server, bplus_server)
    run_update_demo(linear_db, bplus_server)
