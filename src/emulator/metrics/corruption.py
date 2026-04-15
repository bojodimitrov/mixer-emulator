import argparse
import json
import sys

from emulator.storage.engine import DbEngine
from emulator.storage.constants import SHARD_COUNT, db_shard_path


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
    """Read corruption metrics aggregated across all SHARD_COUNT shard files."""
    total_records = 0
    corrupted_records = 0

    for i in range(SHARD_COUNT):
        path = db_shard_path(i)
        db = DbEngine(db_path=path, shard_index=i, shard_count=SHARD_COUNT)

        progress_callback = None
        if show_progress:
            shard_label = f"shard {i}"

            def _cb(current: int, total: int, _label: str = shard_label) -> None:
                if total <= 0:
                    return
                bar_width = 24
                ratio = current / total
                filled = min(bar_width, int(ratio * bar_width))
                bar = "#" * filled + "-" * (bar_width - filled)
                print(
                    f"\r[{_label}] [{bar}] {ratio * 100:5.1f}% {current}/{total}",
                    end="" if current < total else "\n",
                    file=sys.stderr,
                    flush=True,
                )

            progress_callback = _cb

        level = db.get_corruption_level(progress_callback=progress_callback)
        total_records += level["total_records"]
        corrupted_records += level["corrupted_records"]

    corruption_percent = (corrupted_records / total_records * 100.0) if total_records > 0 else 0.0
    return {
        "total_records": total_records,
        "corrupted_records": corrupted_records,
        "corruption_percent": round(corruption_percent, 2),
    }


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
