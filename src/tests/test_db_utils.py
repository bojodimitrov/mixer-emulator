"""Tests-only utilities.

This module exists in `src/tests` so "helper" code doesn't leak into the
production `emulator.*` package.

It provides a small helper to create a temporary on-disk `FileDB` populated with
records, and (best-effort) pre-build the B+ tree index file.

Note: This intentionally patches module-level constants in
`emulator.storage.database` (as the production code relies on them).
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass


@dataclass
class SeededDbPaths:
    temp_dir: tempfile.TemporaryDirectory
    db_path: str
    idx_path: str
    bpt_path: str


def create_seeded_temp_db(
    *,
    capacity: int = 128,
    populate_end: int = 128,
    lookup_strategy: str = "linear",
) -> SeededDbPaths:
    """Create a temporary on-disk DB and populate ids [0, populate_end).

    Returns paths + the TemporaryDirectory handle (caller must cleanup).
    """

    from emulator.storage import database as database_module

    td = tempfile.TemporaryDirectory()
    db_path = os.path.join(td.name, "temp.db")
    idx_path = os.path.join(td.name, "temp.idx")
    bpt_path = os.path.join(td.name, "temp.bpt")

    # Patch module-level constants used by FileDB.
    database_module.DEFAULT_DB_PATH = db_path
    database_module.DEFAULT_INDEX_PATH = idx_path
    database_module.DEFAULT_BPLUS_INDEX_PATH = bpt_path

    db = database_module.FileDB(lookup_strategy=lookup_strategy)  # type: ignore[arg-type]
    db.ensure_capacity(int(capacity))
    db.populate_range(0, int(populate_end))

    # If the caller plans to use B+ lookup strategy, build a small B+ index.
    # This keeps socket-based tests self-contained.
    try:
        from emulator.storage.b_tree_index import build_bplus_tree

        build_bplus_tree(db_path=db_path, out_path=bpt_path)
    except Exception:
        # Index is an optimization; callers that don't need it can ignore.
        pass

    return SeededDbPaths(temp_dir=td, db_path=db_path, idx_path=idx_path, bpt_path=bpt_path)
