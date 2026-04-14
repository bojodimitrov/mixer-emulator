import argparse
import json
import sys

from emulator.storage.engine import DbEngine


def _print_progress(current: int, total: int) -> None:
    if total <= 0:
        print("\rScanning corruption: 0/0", end="", file=sys.stderr, flush=True)
        return

    bar_width = 30
    ratio = current / total
    filled = min(bar_width, int(ratio * bar_width))
    bar = "#" * filled + "-" * (bar_width - filled)
    print(
        f"\rScanning corruption [{bar}] {ratio * 100:6.2f}% {current}/{total}",
        end="" if current < total else "\n",
        file=sys.stderr,
        flush=True,
    )


def get_database_corruption_level(*, show_progress: bool = False) -> dict[str, float | int]:
    """Read corruption metrics directly from the database engine."""
    db = DbEngine()
    progress_callback = _print_progress if show_progress else None
    return db.get_corruption_level(progress_callback=progress_callback)


def print_database_corruption_level(*, show_progress: bool = False) -> None:
    metrics = get_database_corruption_level(show_progress=show_progress)
    print(json.dumps(metrics, indent=2, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print current database corruption metrics."
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable the loading/progress indicator while scanning the database.",
    )
    args = parser.parse_args()
    print_database_corruption_level(show_progress=not args.no_progress)


if __name__ == "__main__":
    main()
