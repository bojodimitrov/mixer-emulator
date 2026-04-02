import random
from typing import Tuple

from ..storage.engine import DbEngine
from ..storage.orchestrator import DbRequest, DbOrchestrator
from ..utils import compute_hash_for, print_time


def _measure_lookup(server: DbOrchestrator, strategy_name: str, hash_hex: str) -> None:
    print_time(
        f"Lookup for {strategy_name}",
        lambda: server.handle_request(DbRequest("Query", {"hash": hash_hex})),
    )
    print()


def create_servers() -> Tuple[DbOrchestrator, DbOrchestrator]:
    return (
        DbOrchestrator(lookup_strategy=DbEngine.STRATEGY_LINEAR),
        DbOrchestrator(lookup_strategy=DbEngine.STRATEGY_BPLUS),
    )


def run_lookup_demo(
    db_engine: DbEngine,
    linear_server: DbOrchestrator,
    bplus_server: DbOrchestrator,
) -> None:
    record_count = db_engine.record_count()
    record_id = random.randint(0, record_count - 1)

    id_read, name, hash_hex = print_time(
        "Read record", lambda: db_engine.read_record(record_id)
    )
    print(f"id={id_read} name={name} hash={hash_hex}")

    _measure_lookup(linear_server, "linear", hash_hex)
    _measure_lookup(bplus_server, "bplus", hash_hex)


def run_update_demo(linear_db: DbEngine, bplus_server: DbOrchestrator) -> None:
    record_count = linear_db.record_count()

    update_id = random.randint(0, record_count - 1)
    _, old_update_name, old_update_hash = linear_db.read_record(update_id)
    updated_name = "zzzzz" if old_update_name != "zzzzz" else "yyyyy"

    print_time(
        f"B+ update",
        lambda: bplus_server.handle_request(
            DbRequest("Command", {"id": update_id, "new_name": updated_name})
        ),
    )

    updated_result = bplus_server.handle_request(
        DbRequest(
            "Query",
            {"hash": compute_hash_for(update_id, updated_name.encode("ascii")).hex()},
        )
    )
    old_result = bplus_server.handle_request(
        DbRequest("Query", {"hash": old_update_hash})
    )
    print(f"bplus verify new hash -> {updated_result}")
    print(f"bplus verify old hash -> {old_result}")


def run_database_demo() -> None:
    linear_db = DbEngine(lookup_strategy=DbEngine.STRATEGY_LINEAR)
    linear_server, bplus_server = create_servers()

    record_count = linear_db.record_count()
    record_id = random.randint(0, record_count - 1)

    _, _, hash_hex = print_time(
        f"Read record {record_id}",
        lambda: linear_db.read_record(record_id),
    )

    _measure_lookup(bplus_server, "bplus", hash_hex)
    _measure_lookup(bplus_server, "bplus", hash_hex)
    _measure_lookup(bplus_server, "bplus", hash_hex)

    # run_lookup_demo(linear_db, linear_server, bplus_server)
    # run_update_demo(linear_db, bplus_server)
