"""Demo: Corrupter corrupts a record, then Repairer repairs it (socket-based).

Run:
  python -m emulator.demonstrations.corrupt_and_repair_demo

This starts:
  - DbServer
  - MicroserviceServer

Then it runs:
  1) Corrupter once for a chosen id
  2) Repairer once for the same id
"""

from __future__ import annotations

import random

from emulator.frontend.clients import Corrupter, Repairer
from emulator.utils import print_time

from ..microservice.server import MicroserviceServer
from ..storage.engine import DbEngine
from ..storage.server import DbServer


def run_demo(*, seed: int | None = None) -> None:
    if seed is not None:
        random.seed(int(seed))

    db = DbEngine()
    record_id = random.randint(0, max(0, db.record_count() - 1))

    db_server = DbServer()
    db_server.start()

    svc_server = MicroserviceServer()
    svc_server.start()

    try:
        print("== Corrupt + repair demo ==")
        print(f"service: 127.0.0.1:{svc_server.port}")
        print(f"db:      127.0.0.1:{db_server.port}")
        print(f"target id: {record_id}")

        corrupter = Corrupter()
        repairer = Repairer()

        _id_read, name, hash = db.read_record(record_id)
        print(f"Record to be changed: id= {_id_read} name= {name} hash= {hash}")
        print()

        print_time("Corrupt call", lambda: corrupter.run_once(record_id=record_id))

        _id_read, name, hash = db.read_record(record_id)
        print(f"Record corrupted: id= {_id_read} name= {name} hash= {hash}")
        print()

        print_time("Repair call", lambda: repairer.run_once(record_id=record_id))
        print_time("Repair call", lambda: repairer.run_once(record_id=record_id))

        _id_read, name, hash = db.read_record(record_id)
        print(f"Record repaired: id= {_id_read} name= {name} hash= {hash}")

    finally:
        svc_server.close()
        db_server.close()


if __name__ == "__main__":
    run_demo()
