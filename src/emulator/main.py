from __future__ import annotations

import argparse
import resource
from typing import Dict


def _raise_fd_limit() -> None:
    """Raise the process soft fd limit to the hard limit (best-effort).

    macOS shells default to ulimit -n 256. Server workloads with many worker
    threads and connection pools exhaust this quickly. Bumping to the hard
    limit (typically 10 000+ on macOS, unlimited on Linux) before starting
    any sockets avoids EMFILE errors entirely.
    """
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        # Cap at 65536 in case hard is RLIM_INFINITY (2^63-1 on Linux).
        desired = min(hard, 65536) if hard > 0 and hard < 2 ** 62 else 65536
        if soft < desired:
            resource.setrlimit(resource.RLIMIT_NOFILE, (desired, hard))
    except (ValueError, OSError):
        pass  # Best-effort — continue with whatever the shell gave us.

from .orchestrator.monitor import run_headless_monitor, run_metrics_window
from .orchestrator.runtime import SystemOrchestrator
from .storage.engine import DbEngine
from .storage.orchestrator import LookupStrategy


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run emulator system orchestrator")
    parser.add_argument("--corrupters", type=int, default=2)
    parser.add_argument("--repairers", type=int, default=2)
    parser.add_argument("--client-pause-ms", type=float, default=500.0)
    parser.add_argument("--ramp-up-step-sec", type=float, default=0.0,
                        help="Seconds between each 20 %% ramp-up step (0 = no ramp-up)")
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
    _raise_fd_limit()
    args = _parse_args()

    orchestrator = SystemOrchestrator(
        db_lookup_strategy=_lookup_strategy_from_arg(args.lookup_strategy),
        corrupter_count=args.corrupters,
        repairer_count=args.repairers,
        client_pause_ms=args.client_pause_ms,
        ramp_up_step_sec=args.ramp_up_step_sec,
    )
    orchestrator.start()

    if args.headless:
        run_headless_monitor(orchestrator, duration_sec=args.duration_sec)
        return

    run_metrics_window(orchestrator)


if __name__ == "__main__":
    run_app()
