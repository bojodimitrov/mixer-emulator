"""Socket-based demo that runs ONLY B+ tree Query + Command operations.

This demo starts a TCP database server configured for the B+ tree lookup strategy,
then runs:
  1) Query(hash)  -> returns (id, name)
  2) Command(id)  -> updates the record name
  3) Query(old_hash) -> should return None
  4) Query(new_hash) -> should return updated (id, name)

Note: You must have a database file and B+ tree index built already.

Build steps (optional):
  - python -m emulator.storage.database --start 0 --end 100000
  - python -m emulator.storage.b_tree_index

Run:
  - python -m emulator.demonstrations.socket_bplus_demo
"""

from __future__ import annotations

import random

from ..storage.engine import DbEngine
from ..storage.client import DbClient
from ..storage.server import DbServer
from ..utils import print_time


def run_socket_bplus_demo() -> None:
    # Pick a random record from the DB (read directly from the on-disk DB file).
    db = DbEngine()
    record_count = db.record_count()
    if record_count <= 0:
        raise RuntimeError(
            "database is empty; build it first with `python -m emulator.storage.database`"
        )

    record_id = random.randint(0, record_count - 1)
    _id_read, old_name, old_hash = db.read_record(record_id)

    # Start socket DB server.
    server = DbServer()
    server.start()
    try:
        client = DbClient(pool_size=20)

        print("== B+ tree socket demo ==")
        print(f"record_id= {record_id} name= {old_name} hash= {old_hash}")

        for _ in range(3):
            print_time("Query", lambda: client.query(old_hash))

    finally:
        client.close()
        server.close()


if __name__ == "__main__":
    run_socket_bplus_demo()
