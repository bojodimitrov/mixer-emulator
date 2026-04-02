from __future__ import annotations

import argparse
from typing import Dict

from .orchestrator.monitor import run_headless_monitor, run_metrics_window
from .orchestrator.runtime import SystemOrchestrator
from .storage.engine import DbEngine
from .storage.orchestrator import LookupStrategy


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run emulator system orchestrator")
    parser.add_argument("--corrupters", type=int, default=2)
    parser.add_argument("--repairers", type=int, default=2)
    parser.add_argument("--client-pause-ms", type=float, default=100.0)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--duration-sec", type=float, default=None)
    parser.add_argument(
        "--lookup-strategy",
        choices=["linear", "bplus"],
        default="bplus",
    )
    return parser.parse_args()


def _lookup_strategy_from_arg(name: str) -> LookupStrategy:
    mapping: Dict[str, LookupStrategy] = {
        "linear": DbEngine.STRATEGY_LINEAR,
        "bplus": DbEngine.STRATEGY_BPLUS,
    }
    return mapping[name]


def run_app() -> None:
    args = _parse_args()

    orchestrator = SystemOrchestrator(
        db_lookup_strategy=_lookup_strategy_from_arg(args.lookup_strategy),
        corrupter_count=args.corrupters,
        repairer_count=args.repairers,
        client_pause_ms=args.client_pause_ms,
    )
    orchestrator.start()

    if args.headless:
        run_headless_monitor(orchestrator, duration_sec=args.duration_sec)
        return

    run_metrics_window(orchestrator)


if __name__ == "__main__":
    run_app()
