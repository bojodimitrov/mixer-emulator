import argparse
import json

from emulator.storage.engine import DbEngine


def get_database_corruption_level() -> dict[str, float | int]:
    """Read corruption metrics directly from the database engine."""
    db = DbEngine()
    return db.get_corruption_level()


def print_database_corruption_level() -> None:
    metrics = get_database_corruption_level()
    print(json.dumps(metrics, indent=2, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print current database corruption metrics."
    )
    parser.parse_args()
    print_database_corruption_level()


if __name__ == "__main__":
    main()
