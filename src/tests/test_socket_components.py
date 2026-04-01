import os
import tempfile
import time
import unittest
from unittest.mock import patch

import emulator.storage.database as database_module
from emulator.storage.socket_client import SocketDatabaseClient
from emulator.storage.socket_server import SocketDatabaseServer
from emulator.socket_microservice import SocketMicroserviceClient, SocketMicroserviceServer


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

        self.db = database_module.FileDB()
        self.db.ensure_capacity(64)
        self.db.populate_range(0, 64)

        # Use ephemeral port to avoid collisions.
        self.db_server = SocketDatabaseServer(host="127.0.0.1", port=0)
        self.db_server.start()
        self.addCleanup(self.db_server.close)

        # Give the thread a moment to bind.
        time.sleep(0.02)
        assert self.db_server._socket is not None
        self.db_port = int(self.db_server._socket.getsockname()[1])

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_query_and_command_roundtrip(self):
        client = SocketDatabaseClient("127.0.0.1", self.db_port)

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
        client = SocketDatabaseClient(
            "127.0.0.1",
            self.db_port,
            keepalive=True,
        )
        self.addCleanup(client.close)

        from emulator.utils import compute_hash_for, id_to_name

        record_id = 1
        name_b = id_to_name(record_id)
        h = compute_hash_for(record_id, name_b)

        # Two requests on same TCP connection.
        self.assertIsNotNone(client.query(h))
        self.assertIsNotNone(client.query(h))

        client.close()
        # After close, the client should be able to open a new connection and work.
        self.assertIsNotNone(client.query(h))


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

        self.db = database_module.FileDB()
        self.db.ensure_capacity(32)
        self.db.populate_range(0, 32)

        self.db_server = SocketDatabaseServer(host="127.0.0.1", port=0)
        self.db_server.start()
        self.addCleanup(self.db_server.close)

        time.sleep(0.02)
        assert self.db_server._socket is not None
        db_port = int(self.db_server._socket.getsockname()[1])

        self.svc_server = SocketMicroserviceServer(
            host="127.0.0.1",
            port=0,
            db_host="127.0.0.1",
            db_port=db_port,
            latency_ms=0,
            pool_size=5,
        )
        self.svc_server.start()
        self.addCleanup(self.svc_server.close)

        time.sleep(0.02)
        assert self.svc_server._sock is not None
        self.svc_port = int(self.svc_server._sock.getsockname()[1])

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_client_times_out_cleanly(self):
        # Uses a tiny timeout and a guaranteed-bad port.
        bad = SocketMicroserviceClient("127.0.0.1", 1, timeout_sec=0.05)
        from emulator.utils import compute_hash_for, id_to_name

        record_id = 0
        h = compute_hash_for(record_id, id_to_name(record_id))

        with self.assertRaises(Exception):
            bad.get(h)

    def test_server_rejects_invalid_request(self):
        # Send a raw message with invalid method through the transport layer.
        from emulator.transport import TcpEndpoint, send_message, recv_message

        ep = TcpEndpoint("127.0.0.1", self.svc_port)
        with ep.connect(timeout_sec=1.0) as sock:
            send_message(sock, {"method": "BOGUS"})
            resp = recv_message(sock)
        self.assertEqual(resp.get("status"), "error")


if __name__ == "__main__":
    unittest.main()
