"""Demo: Gets by different record IDs to compare speed.

Run:
  python -m emulator.demonstrations.gets_demo

This starts:
  - DbServer
  - MicroserviceServer

"""

from __future__ import annotations

import random

from emulator.frontend.clients import Repairer
from emulator.utils import print_time

from ..microservice.server import MicroserviceServer
from ..storage.engine import DbEngine
from ..storage.server import DbServer


def run_demo(*, seed: int | None = None) -> None:
    if seed is not None:
        random.seed(int(seed))

    db = DbEngine(lookup_strategy=DbEngine.STRATEGY_LINEAR)
    total_records = db.record_count()
    if total_records <= 0:
        print("No records found; skipping GET demo.")
        return

    sample_size = min(10, total_records)
    record_ids = random.sample(range(total_records), k=sample_size)

    db_server = DbServer(lookup_strategy=DbEngine.STRATEGY_BPLUS, conn_timeout_sec=30)
    db_server.start()

    svc_server = MicroserviceServer(
        latency_ms=0,
    )
    svc_server.start()

    try:
        print("== Get different records demo ==")
        print(f"service: 127.0.0.1:{svc_server.port}")
        print(f"db:      127.0.0.1:{db_server.port}")
        print(f"target ids ({len(record_ids)}): {record_ids}")

        repairer = Repairer(timeout_sec=30)

        for rid in record_ids:
            print_time(
                f"Get call id={rid}", lambda rid=rid: repairer.run_once(record_id=rid)
            )

    finally:
        svc_server.close()
        db_server.close()


if __name__ == "__main__":
    run_demo()
