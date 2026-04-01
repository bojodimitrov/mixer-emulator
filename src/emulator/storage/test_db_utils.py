"""Utilities for creating a seeded, temporary FileDB for demos/tests.

The core `FileDB` implementation uses module-level constants for DB/index paths.
For reliable tests and socket demos, we patch those constants to point at a
temporary directory and populate a small range of records.

This module is intentionally lightweight and avoids pytest-specific fixtures so
it can be used from unittest-based tests and ad-hoc demonstrations.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from typing import Optional


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

    from . import database as database_module

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
    # This keeps socket-based demos/tests self-contained.
    try:
        from .b_tree_index import build_bplus_tree

        build_bplus_tree(db_path=db_path, out_path=bpt_path)
    except Exception:
        # Index is an optimization; callers that don't need it can ignore.
        pass

    return SeededDbPaths(temp_dir=td, db_path=db_path, idx_path=idx_path, bpt_path=bpt_path)
