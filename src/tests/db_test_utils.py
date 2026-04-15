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
import time
from dataclasses import dataclass
from typing import List, Optional

import emulator.microservice.framework as _framework_module
from emulator.servers_config import DbEndpoint as _DbEndpoint


@dataclass
class SeededDbPaths:
    temp_dir: tempfile.TemporaryDirectory
    db_path: str
    bpt_path: str


def create_seeded_temp_db(
    *,
    capacity: int = 128,
    populate_end: int = 128,
) -> SeededDbPaths:
    """Create a temporary on-disk DB and populate ids [0, populate_end).

    Returns paths + the TemporaryDirectory handle (caller must cleanup).
    """

    from emulator.storage import engine as database_module

    td = tempfile.TemporaryDirectory()
    db_path = os.path.join(td.name, "temp.db")
    bpt_path = os.path.join(td.name, "temp.bpt")

    # Patch module-level constant used by DbEngine.
    database_module.DEFAULT_DB_PATH = db_path

    db = database_module.DbEngine()
    db.ensure_capacity(int(capacity))
    db.populate_range(0, int(populate_end))

    return SeededDbPaths(temp_dir=td, db_path=db_path, bpt_path=bpt_path)


# ---------------------------------------------------------------------------
# Sharded infrastructure helpers
# ---------------------------------------------------------------------------

@dataclass
class SeededShardInfra:
    temp_dir: tempfile.TemporaryDirectory
    shard_db_paths: List[str]
    gsi_path: str


def create_seeded_shard_infra(
    *,
    capacity: int = 64,
    populate_end: int = 64,
) -> SeededShardInfra:
    """Create 4 shard DBs and a GSI B+ tree index file in a temp directory.

    The GSI is built from a flat (non-sharded) copy of the same records.
    Because record names and hashes are deterministic (id_to_name / compute_hash_for),
    the resulting index correctly maps hash → global_id for the sharded DBs too.
    """
    from emulator.storage import engine as database_module
    from emulator.storage.b_tree_index import build_bplus_tree
    from emulator.storage.constants import SHARD_COUNT

    td = tempfile.TemporaryDirectory()
    base = td.name
    shard_paths: List[str] = []

    for i in range(SHARD_COUNT):
        shard_dir = os.path.join(base, f"shard_{i}")
        os.makedirs(shard_dir)
        db_path = os.path.join(shard_dir, "records.db")
        shard_paths.append(db_path)
        db = database_module.DbEngine(db_path=db_path, shard_index=i, shard_count=SHARD_COUNT)
        db.ensure_capacity(capacity)
        db.populate_range(0, populate_end)

    # Build GSI from a flat non-sharded DB.
    flat_db_path = os.path.join(base, "flat.db")
    flat_db = database_module.DbEngine(db_path=flat_db_path)
    flat_db.ensure_capacity(capacity)
    flat_db.populate_range(0, populate_end)

    gsi_path = os.path.join(base, "gsi.bpt")
    build_bplus_tree(db_path=flat_db_path, out_path=gsi_path)

    return SeededShardInfra(temp_dir=td, shard_db_paths=shard_paths, gsi_path=gsi_path)


class ShardHarness:
    """Start a GsiServer + 4 shard DbServers on ephemeral ports and patch the
    framework-level endpoint constants so that any ``CustomApi`` created while
    this harness is active connects to those test servers.

    Usage in setUp / tearDown::

        def setUp(self):
            self._harness = ShardHarness()
            self._harness.start()
            self._harness.register_cleanup(self)   # auto cleanup via addCleanup
            self.svc = MicroserviceServer(...)      # CustomApi picks up patched ports
            self.svc.start()

    Usage as a context manager::

        with ShardHarness() as harness:
            svc = MicroserviceServer(...)
            svc.start()
            ...
    """

    def __init__(self, capacity: int = 64, populate_end: int = 64) -> None:
        self._capacity = capacity
        self._populate_end = populate_end
        self._infra: Optional[SeededShardInfra] = None
        self._gsi_server = None
        self._shard_servers: List = []
        self._patches: List = []

    def start(self) -> None:
        """Seed data, start servers, and patch framework endpoint constants."""
        from emulator.storage.gsi_server import GsiServer
        from emulator.storage.server import DbServer
        from emulator.storage.constants import SHARD_COUNT
        from unittest.mock import patch

        self._infra = create_seeded_shard_infra(
            capacity=self._capacity, populate_end=self._populate_end
        )

        self._gsi_server = GsiServer(index_path=self._infra.gsi_path, port=0)
        self._gsi_server.start()
        gsi_port = self._gsi_server._socket.getsockname()[1]

        shard_ports: List[int] = []
        for i in range(SHARD_COUNT):
            srv = DbServer(
                db_path=self._infra.shard_db_paths[i],
                shard_index=i,
                shard_count=SHARD_COUNT,
                port=0,
            )
            srv.start()
            shard_ports.append(srv._socket.getsockname()[1])
            self._shard_servers.append(srv)

        gsi_ep = _DbEndpoint(host="127.0.0.1", port=gsi_port)
        shard_eps = [_DbEndpoint(host="127.0.0.1", port=p) for p in shard_ports]

        p1 = patch.object(_framework_module, "GSI_ENDPOINT", gsi_ep)
        p2 = patch.object(_framework_module, "DB_SHARD_ENDPOINTS", shard_eps)
        p1.start()
        p2.start()
        self._patches = [p1, p2]

        time.sleep(0.03)  # brief readiness pause

    def stop(self) -> None:
        """Stop all servers, restore patches, and clean up temp files."""
        for p in reversed(self._patches):
            try:
                p.stop()
            except Exception:
                pass
        self._patches.clear()

        for srv in reversed(self._shard_servers):
            try:
                srv.close()
            except Exception:
                pass
        self._shard_servers.clear()

        if self._gsi_server is not None:
            try:
                self._gsi_server.close()
            except Exception:
                pass
            self._gsi_server = None

        if self._infra is not None:
            try:
                self._infra.temp_dir.cleanup()
            except Exception:
                pass
            self._infra = None

    def register_cleanup(self, test_case) -> None:
        """Register stop() as an addCleanup callback on a TestCase."""
        test_case.addCleanup(self.stop)

    # context-manager support
    def __enter__(self) -> "ShardHarness":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()
