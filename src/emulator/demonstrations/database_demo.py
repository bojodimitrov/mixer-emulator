import random

from ..storage.engine import DbEngine
from ..storage.orchestrator import DbRequest, DbOrchestrator
from ..utils import compute_hash_for, print_time


def _measure_lookup(server: DbOrchestrator, hash_hex: str) -> None:
    print_time(
        "Lookup (linear scan)",
        lambda: server.handle_request(DbRequest("Query", {"hash": hash_hex})),
    )
    print()


def create_server() -> DbOrchestrator:
    return DbOrchestrator()


def run_lookup_demo(db_engine: DbEngine, server: DbOrchestrator) -> None:
    record_count = db_engine.record_count()
    record_id = random.randint(0, record_count - 1)

    id_read, name, hash_hex = print_time(
        "Read record", lambda: db_engine.read_record(record_id)
    )
    print(f"id={id_read} name={name} hash={hash_hex}")

    _measure_lookup(server, hash_hex)


def run_update_demo(db: DbEngine, server: DbOrchestrator) -> None:
    record_count = db.record_count()

    update_id = random.randint(0, record_count - 1)
    _, old_update_name, old_update_hash = db.read_record(update_id)
    updated_name = "zzzzz" if old_update_name != "zzzzz" else "yyyyy"

    print_time(
        "Update",
        lambda: server.handle_request(
            DbRequest("Command", {"id": update_id, "new_name": updated_name})
        ),
    )

    updated_result = server.handle_request(
        DbRequest(
            "Query",
            {"hash": compute_hash_for(update_id, updated_name.encode("ascii")).hex()},
        )
    )
    old_result = server.handle_request(
        DbRequest("Query", {"hash": old_update_hash})
    )
    print(f"verify new hash -> {updated_result}")
    print(f"verify old hash -> {old_result}")


def run_database_demo() -> None:
    db = DbEngine()
    server = create_server()

    record_count = db.record_count()
    record_id = random.randint(0, record_count - 1)

    _, _, hash_hex = print_time(
        f"Read record {record_id}",
        lambda: db.read_record(record_id),
    )

    _measure_lookup(server, hash_hex)
    _measure_lookup(server, hash_hex)
    _measure_lookup(server, hash_hex)
