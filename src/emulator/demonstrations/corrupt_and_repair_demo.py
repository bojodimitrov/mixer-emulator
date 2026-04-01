"""Demo: Corrupter corrupts a record, then Repairer repairs it (socket-based).

Run:
  python -m emulator.demonstrations.socket_corrupt_and_repair_demo

This starts:
  - SocketDatabaseServer (ephemeral port)
  - SocketMicroserviceServer (ephemeral port)

Then it runs:
  1) Corrupter once for a chosen id
  2) Repairer once for the same id

Both Corrupter and Repairer are socket-only (they own a SocketMicroserviceClient
internally and do not accept in-process service injection).
"""

from __future__ import annotations

import importlib
import random

from ..socket_microservice import SocketMicroserviceServer
from ..storage.database import FileDB
from ..storage.socket_server import SocketDatabaseServer

frontend_clients = importlib.import_module("emulator.frontend-clients")
Corrupter = frontend_clients.Corrupter
Repairer = frontend_clients.Repairer


def run_demo(*, seed: int | None = None) -> None:
    if seed is not None:
        random.seed(int(seed))

    db = FileDB(lookup_strategy=FileDB.STRATEGY_LINEAR)
    record_id = random.randint(0, max(0, db.record_count() - 1))

    db_server = SocketDatabaseServer(
        lookup_strategy=FileDB.STRATEGY_BPLUS,
    )
    db_server.start()

    svc_server = SocketMicroserviceServer(
        latency_ms=0,
        pool_size=50,
    )
    svc_server.start()


    try:
        print("== Socket corrupt + repair demo ==")
        print(f"service: 127.0.0.1:{svc_server.port}")
        print(f"db:      127.0.0.1:{db_server.port}")
        print(f"target id: {record_id}")

        corrupter = Corrupter()
        repairer = Repairer()

        _id_read, name, hash = db.read_record(record_id)
        print(f"Record to be changed: id= {_id_read} name= {name} hash= {hash.hex()}")
        print()

        corrupt_resp = corrupter.run_once(record_id=record_id)
        print(f"corrupter response: {corrupt_resp}")

        _id_read, name, hash = db.read_record(record_id)
        print(f"Record corrupted: id= {_id_read} name= {name} hash= {hash.hex()}")
        print()

        repair_resp = repairer.run_once(record_id=record_id)
        print(f"repairer response:  {repair_resp}")

        _id_read, name, hash = db.read_record(record_id)
        print(f"Record repaired: id= {_id_read} name= {name} hash= {hash.hex()}")

    finally:
        svc_server.close()
        db_server.close()


if __name__ == "__main__":
    run_demo()
