import time
import unittest
from unittest.mock import patch

from emulator.microservice.client import MicroserviceClient
from emulator.microservice.server import MicroserviceServer
from emulator.utils import compute_hash_for, id_to_name


class _FakeSocket:
    def __init__(self):
        self._fileno = 123
        self.closed = False

    def fileno(self):
        return self._fileno

    def close(self):
        self.closed = True
        self._fileno = -1

    def settimeout(self, _timeout):
        return None


class TestSocketDecoupling(unittest.TestCase):
    def setUp(self):
        from tests.db_test_utils import ShardHarness

        self._shard_harness = ShardHarness(capacity=64, populate_end=64)
        self._shard_harness.start()
        self._shard_harness.register_cleanup(self)

        self.svc_server = MicroserviceServer(
            latency_ms=0,
            pool_size=10,
        )
        self.svc_server.start()
        self.addCleanup(self.svc_server.close)

        # crude readiness wait
        time.sleep(0.05)

    def test_query_and_update_over_sockets(self):
        from emulator.transport_layer.transport import hex_from_bytes

        client = MicroserviceClient(host="127.0.0.1", port=self.svc_server.port)

        record_id = 7
        name_b = id_to_name(record_id)
        h = compute_hash_for(record_id, name_b)

        resp = client.request("GET", {"hash": hex_from_bytes(h)}, "/hash")
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], [record_id, name_b.decode("ascii")])

        resp2 = client.request("POST", {"id": record_id, "new_name": "zzzzz"}, "/name")
        self.assertEqual(resp2["status"], "ok")
        self.assertEqual(resp2["result"], {"updated": True})

        # Allow the fire-and-forget GSI update to propagate.
        time.sleep(0.05)

        # old hash should no longer match
        resp3 = client.request("GET", {"hash": hex_from_bytes(h)}, "/hash")
        self.assertEqual(resp3["status"], "ok")
        self.assertIsNone(resp3["result"])

    def test_unsupported_method_returns_error(self):
        client = MicroserviceClient(host="127.0.0.1", port=self.svc_server.port)

        resp = client.request("PUT", {"id": 7, "new_name": "zzzzz"}, "/name")
        self.assertEqual(resp.get("status"), "error")
        self.assertIn("unsupported method", str(resp.get("error", "")).lower())

    def test_unknown_path_returns_error(self):
        from emulator.transport_layer.transport import hex_from_bytes

        client = MicroserviceClient(host="127.0.0.1", port=self.svc_server.port)
        record_id = 7
        h = compute_hash_for(record_id, id_to_name(record_id))

        resp = client.request("GET", {"hash": hex_from_bytes(h)}, path="/unknown")
        self.assertEqual(resp.get("status"), "error")
        self.assertIn("unsupported route", str(resp.get("error", "")).lower())

    def test_microservice_client_pool_reuses_socket(self):
        from emulator.transport_layer.transport import hex_from_bytes

        client = MicroserviceClient(pool_size=2, host="127.0.0.1", port=self.svc_server.port)
        self.addCleanup(client.close)

        record_id = 7
        h = compute_hash_for(record_id, id_to_name(record_id))

        # Sequential requests should reuse one pooled socket instead of creating new sockets.
        for _ in range(12):
            resp = client.request("GET", {"hash": hex_from_bytes(h)}, "/hash")
            self.assertEqual(resp.get("status"), "ok")

        self.assertEqual(client._created, 1)
        self.assertIsNotNone(client._pool)
        pool = client._pool
        assert pool is not None
        self.assertEqual(pool.qsize(), 1)


class TestMicroserviceClientPoolAging(unittest.TestCase):
    def test_pool_recycles_socket_past_idle_timeout(self):
        client = MicroserviceClient(pool_size=1, max_idle_sec=0.01)
        self.addCleanup(client.close)

        stale = _FakeSocket()
        fresh = _FakeSocket()
        assert client._pool is not None

        now = time.monotonic()
        client._created = 1
        client._socket_timestamps[id(stale)] = (now - 1.0, now - 0.1)
        client._pool.put_nowait(stale)  # type: ignore[arg-type]

        with patch(
            "emulator.transport_layer.transport.TcpEndpoint.connect",
            return_value=fresh,
        ):
            got = client._pool_acquire()

        self.assertIs(got, fresh)
        self.assertTrue(stale.closed)
        self.assertEqual(client._created, 1)


if __name__ == "__main__":
    unittest.main()
