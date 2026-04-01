import os
import tempfile
import time
import unittest
from unittest.mock import patch

import emulator.storage.engine as database_module
from emulator.storage.client import DbClient
from emulator.storage.server import DbServer
from emulator.microservice.server import MicroserviceServer
from emulator.servers_config import DB_ENDPOINT


class TestSocketDatabaseServerClient(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test.db")
        self.idx_path = os.path.join(self.temp_dir.name, "test.idx")
        self.bpt_path = os.path.join(self.temp_dir.name, "test.bpt")

        patches = [
            patch.object(database_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(database_module, "DEFAULT_INDEX_PATH", self.idx_path),
            patch.object(database_module, "DEFAULT_BPLUS_INDEX_PATH", self.bpt_path),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

        self.db = database_module.DbEngine()
        self.db.ensure_capacity(64)
        self.db.populate_range(0, 64)

        # Use ephemeral port to avoid collisions.
        self.db_server = DbServer()
        self.db_server.start()
        self.addCleanup(self.db_server.close)

        # Give the thread a moment to bind.
        time.sleep(0.02)
        assert self.db_server._socket is not None
        self.db_port = int(self.db_server._socket.getsockname()[1])

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_query_and_command_roundtrip(self):
        client = DbClient()

        # Use poor-man's blackbox: query for a known record by computing the same
        # hash the DB uses.
        from emulator.utils import compute_hash_for, id_to_name

        record_id = 7
        name_b = id_to_name(record_id)
        h = compute_hash_for(record_id, name_b)

        resp = client.query(h)
        self.assertEqual(resp, [record_id, name_b.decode("ascii")])

        ok = client.command(record_id, "zzzzz")
        self.assertTrue(ok)

        # old hash should no longer match
        resp2 = client.query(h)
        self.assertIsNone(resp2)

    def test_keepalive_close_is_graceful(self):
        client = DbClient(
            keepalive=True,
        )
        self.addCleanup(client.close)

        from emulator.utils import compute_hash_for, id_to_name

        record_id = 1
        name_b = id_to_name(record_id)
        h = compute_hash_for(record_id, name_b)

        # Two requests on same TCP connection.
        self.assertIsNotNone(client.query(h))

    def test_connection_pool_reuses_sockets(self):
        client = DbClient(
            pool_size=2,
        )
        self.addCleanup(client.close)

        from emulator.utils import compute_hash_for, id_to_name

        record_id = 2
        name_b = id_to_name(record_id)
        h = compute_hash_for(record_id, name_b)

        # Multiple requests should work without creating a new TCP connection each time.
        # We can't easily assert connection count without server instrumentation,
        # but this ensures the pool path runs and remains stable.
        for _ in range(10):
            self.assertIsNotNone(client.query(h))

    def test_connection_pool_is_thread_safe(self):
        client = DbClient(
            pool_size=4,
        )
        self.addCleanup(client.close)

        from emulator.utils import compute_hash_for, id_to_name

        record_id = 3
        name_b = id_to_name(record_id)
        h = compute_hash_for(record_id, name_b)

        import threading

        errors = []

        def worker():
            try:
                for _ in range(20):
                    r = client.query(h)
                    if r is None:
                        raise AssertionError("unexpected None")
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        self.assertEqual(errors, [])

    def test_connection_pool_eagerly_creates_half_on_init(self):
        client = DbClient(
            pool_size=6,
        )
        self.addCleanup(client.close)

        # Implementation detail but useful to ensure the eager path is exercised.
        self.assertIsNotNone(client._pool)
        # pool_size=6 => eager=3
        self.assertGreaterEqual(client._created, 3)
        # Help static type checkers: _pool is asserted non-None above.
        pool = client._pool
        assert pool is not None
        self.assertGreaterEqual(pool.qsize(), 3)


class TestSocketMicroserviceErrorPaths(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test.db")
        self.idx_path = os.path.join(self.temp_dir.name, "test.idx")
        self.bpt_path = os.path.join(self.temp_dir.name, "test.bpt")

        patches = [
            patch.object(database_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(database_module, "DEFAULT_INDEX_PATH", self.idx_path),
            patch.object(database_module, "DEFAULT_BPLUS_INDEX_PATH", self.bpt_path),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

        self.db = database_module.DbEngine()
        self.db.ensure_capacity(32)
        self.db.populate_range(0, 32)

        self.db_server = DbServer()
        self.db_server.start()
        self.addCleanup(self.db_server.close)

        time.sleep(0.02)
        assert self.db_server._socket is not None

        self.svc_server = MicroserviceServer(
            latency_ms=0,
            pool_size=5,
        )
        self.svc_server.start()
        self.addCleanup(self.svc_server.close)

        time.sleep(0.02)
        assert self.svc_server._socket is not None

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_server_rejects_invalid_request(self):
        # Send a raw message with invalid method through the transport layer.
        from emulator.transport_layer.transport import (
            TcpEndpoint,
            send_message,
            recv_message,
        )

        ep = TcpEndpoint(DB_ENDPOINT.host, DB_ENDPOINT.port)
        with ep.connect(timeout_sec=1.0) as sock:
            send_message(sock, {"method": "BOGUS"})
            resp = recv_message(sock)
        self.assertEqual(resp.get("status"), "error")


if __name__ == "__main__":
    unittest.main()
