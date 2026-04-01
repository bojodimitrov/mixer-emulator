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
import time

from ..storage.database import FileDB
from ..storage.socket_client import SocketDatabaseClient
from ..storage.socket_server import SocketDatabaseServer
from ..socket_config import DB_ENDPOINT
from ..utils import compute_hash_for


def run_socket_bplus_demo() -> None:
    # Pick a random record from the DB (read directly from the on-disk DB file).
    db = FileDB(lookup_strategy=FileDB.STRATEGY_LINEAR)
    record_count = db.record_count()
    if record_count <= 0:
        raise RuntimeError(
            "database is empty; build it first with `python -m emulator.storage.database`"
        )

    record_id = random.randint(0, record_count - 1)
    _id_read, old_name, old_hash = db.read_record(record_id)

      # Start socket DB server in B+ mode.
    server = SocketDatabaseServer(lookup_strategy=FileDB.STRATEGY_BPLUS)
    server.start()
    try:
  # Use pooling (thread-safe) instead of keepalive.
  # The client will eagerly create half of pool_size connections on init.
        client = SocketDatabaseClient(pool_size=20)

        print("== B+ tree socket demo ==")
        print(f"record_id= {record_id} name= {old_name} hash= {old_hash.hex()}")

        t0 = time.perf_counter()
        q1 = client.query(old_hash)
        t1 = time.perf_counter()
        print(f"Query(hash) -> {q1} ({(t1 - t0) * 1000:.3f} ms)")

        t0 = time.perf_counter()
        q1 = client.query(old_hash)
        t1 = time.perf_counter()
        print(f"Query(hash) -> {q1} ({(t1 - t0) * 1000:.3f} ms)")

        t0 = time.perf_counter()
        q1 = client.query(old_hash)
        t1 = time.perf_counter()
        print(f"Query(hash) -> {q1} ({(t1 - t0) * 1000:.3f} ms)")

        # new_name = "zzzzz" if old_name != "zzzzz" else "yyyyy"
        # t0 = time.perf_counter()
        # ok = client.command(record_id, new_name)
        # t1 = time.perf_counter()
        # print(f"Command(update_name={new_name}) -> {ok} ({(t1 - t0) * 1000:.3f} ms)")

        # new_hash = compute_hash_for(record_id, new_name.encode("ascii"))

        # q_old = client.query(old_hash)
        # q_new = client.query(new_hash)
        # print(f"Query(old_hash) after update -> {q_old}")
        # print(f"Query(new_hash) after update -> {q_new}")
    finally:
        client.close()
        server.close()


if __name__ == "__main__":
    run_socket_bplus_demo()
