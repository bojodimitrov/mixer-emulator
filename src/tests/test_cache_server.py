import time
import unittest

from emulator.cache.client import CacheClient
from emulator.cache.server import CacheServer


class TestCacheServerClient(unittest.TestCase):
    def setUp(self):
        self.server = CacheServer()
        self.server.start()
        self.addCleanup(self.server.close)
        time.sleep(0.05)

    def test_ping_set_get_delete_roundtrip(self):
        client = CacheClient()
        self.addCleanup(client.close)

        self.assertTrue(client.ping())
        self.assertTrue(client.set("user:1", {"name": "alice", "score": 42}))
        self.assertTrue(client.exists("user:1"))
        self.assertEqual(client.get("user:1"), {"name": "alice", "score": 42})
        self.assertTrue(client.delete("user:1"))
        self.assertFalse(client.exists("user:1"))
        self.assertIsNone(client.get("user:1"))

    def test_mget_returns_values_in_order(self):
        client = CacheClient()
        self.addCleanup(client.close)

        self.assertTrue(client.set("user:1", "alice"))
        self.assertTrue(client.set("user:2", "bob"))

        self.assertEqual(client.mget(["user:1", "missing", "user:2"]), ["alice", None, "bob"])

    def test_incr_behaves_like_counter(self):
        client = CacheClient()
        self.addCleanup(client.close)

        self.assertEqual(client.incr("counter"), 1)
        self.assertEqual(client.incr("counter", 4), 5)
        self.assertEqual(client.get("counter"), 5)

        self.assertTrue(client.set("not-a-number", "hello"))
        with self.assertRaises(RuntimeError):
            client.incr("not-a-number")

    def test_flush_clears_all_keys(self):
        client = CacheClient()
        self.addCleanup(client.close)

        self.assertTrue(client.set("a", 1))
        self.assertTrue(client.set("b", 2))
        self.assertTrue(client.flush())

        self.assertIsNone(client.get("a"))
        self.assertIsNone(client.get("b"))

    def test_key_expires_after_ttl(self):
        client = CacheClient()
        self.addCleanup(client.close)

        self.assertTrue(client.set("session:1", "cached", ttl_sec=0.05))
        self.assertEqual(client.get("session:1"), "cached")

        time.sleep(0.08)
        self.assertIsNone(client.get("session:1"))
        self.assertFalse(client.exists("session:1"))
        self.assertFalse(client.delete("session:1"))

    def test_cache_client_pool_reuses_socket(self):
        client = CacheClient(pool_size=2)
        self.addCleanup(client.close)

        for value in range(12):
            self.assertTrue(client.set("shared", value))
            self.assertEqual(client.get("shared"), value)

        self.assertEqual(client._created, 1)
        self.assertIsNotNone(client._pool)
        pool = client._pool
        assert pool is not None
        self.assertEqual(pool.qsize(), 1)


if __name__ == "__main__":
    unittest.main()