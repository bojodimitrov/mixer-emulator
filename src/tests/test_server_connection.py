import os
import tempfile
import time
import unittest
from unittest.mock import patch

from emulator.microservice.client import MicroserviceClient
import emulator.storage.engine as database_module
from emulator.storage.server import DbServer
from emulator.microservice.server import (
    MicroserviceServer,
)
from emulator.utils import compute_hash_for, id_to_name


class TestSocketDecoupling(unittest.TestCase):
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

        self.db_server = DbServer()
        self.db_server.start()
        self.addCleanup(self.db_server.close)

        self.svc_server = MicroserviceServer(
            latency_ms=0,
            pool_size=10,
        )
        self.svc_server.start()
        self.addCleanup(self.svc_server.close)

        # crude readiness wait
        time.sleep(0.05)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_query_and_update_over_sockets(self):
        from emulator.transport_layer.transport import hex_from_bytes

        client = MicroserviceClient()

        record_id = 7
        name_b = id_to_name(record_id)
        h = compute_hash_for(record_id, name_b)

        resp = client.request("GET", {"hash": hex_from_bytes(h)})
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], [record_id, name_b.decode("ascii")])

        resp2 = client.request("POST", {"id": record_id, "new_name": "zzzzz"})
        self.assertEqual(resp2["status"], "ok")
        self.assertTrue(resp2["result"])

        # old hash should no longer match
        resp3 = client.request("GET", {"hash": hex_from_bytes(h)})
        self.assertEqual(resp3["status"], "ok")
        self.assertIsNone(resp3["result"])

    def test_unsupported_method_returns_error(self):
        client = MicroserviceClient()

        resp = client.request("PUT", {"id": 7, "new_name": "zzzzz"})
        self.assertEqual(resp.get("status"), "error")
        self.assertIn("unsupported method", str(resp.get("error", "")).lower())


if __name__ == "__main__":
    unittest.main()
