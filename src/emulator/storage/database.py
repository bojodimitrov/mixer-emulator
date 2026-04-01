"""Compatibility module for the historical `emulator.storage.database` path.

This keeps CLI usage stable:

    python -m emulator.storage.database

while the implementation lives in `emulator.storage.engine`.
"""

from .engine import DbEngine, create_database, main

__all__ = ["DbEngine", "create_database", "main"]


if __name__ == "__main__":
    main()
