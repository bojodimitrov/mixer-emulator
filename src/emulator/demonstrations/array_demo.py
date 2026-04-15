"""Demo: Gets by different record IDs to compare speed.

Run:
  python -m emulator.demonstrations.array_demo

This starts:
  - DbServer
  - MicroserviceServer

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
    total_records = db.record_count()
    if total_records <= 0:
        print("No records found; skipping GET demo.")
        return

    calls_count = 10
    record_ids = [random.randrange(total_records) for _ in range(calls_count)]

    db_server = DbServer()
    db_server.start()

    svc_server = MicroserviceServer()
    svc_server.start()

    try:
        print("== B+TREE INDEX ==")
        print("== Repair->Corrupt->Repair demo ==")
        print(f"service: 127.0.0.1:{svc_server.port}")
        print(f"db:      127.0.0.1:{db_server.port}")
        print(f"target ids ({len(record_ids)}): {record_ids}")

        corrupter = Corrupter()
        repairer = Repairer()

        print("-- Repair without corrupt data phase --")
        for rid in record_ids:
            print_time(
                f"Repair call id={rid}",
                lambda rid=rid: repairer.run_once(record_id=rid),
            )

        print("-- Corruption phase --")
        for rid in record_ids:
            print_time(
                f"Corrupt call id={rid}",
                lambda rid=rid: corrupter.run_once(record_id=rid),
            )

        print("-- Repair with corrupt data phase --")
        for rid in record_ids:
            print_time(
                f"Repair call id={rid}",
                lambda rid=rid: repairer.run_once(record_id=rid),
            )

    finally:
        svc_server.close()
        db_server.close()


if __name__ == "__main__":
    run_demo()
