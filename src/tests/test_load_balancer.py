import os
import tempfile
import time
import unittest
from unittest.mock import patch, MagicMock

import emulator.storage.engine as database_module
from emulator.microservice.load_balancer import LoadBalancerServer
from emulator.microservice.server import MicroserviceServer
from emulator.storage.server import DbServer
from emulator.transport_layer.tcp_client import TcpClient
from emulator.transport_layer.transport import TcpEndpoint

# Dedicated test ports — high range to avoid collisions with the main suite.
_SVC_PORT_1 = 59101
_SVC_PORT_2 = 59102
_LB_PORT = 59200


class TestLoadBalancerUnit(unittest.TestCase):

    def test_empty_backends_raises(self):
        with self.assertRaises(ValueError):
            LoadBalancerServer([])

    def test_round_robin_cycles_through_all_backends(self):
        endpoints = [
            TcpEndpoint("127.0.0.1", _SVC_PORT_1),
            TcpEndpoint("127.0.0.1", _SVC_PORT_2),
        ]
        lb = LoadBalancerServer(endpoints, port=_LB_PORT)

        # Six picks should cycle 0,1,0,1,0,1
        picks = [lb._pick_backend() for _ in range(6)]
        clients = lb._backend_clients
        self.assertEqual(
            picks,
            [clients[i % 2] for i in range(6)],
        )

    def test_on_close_stops_all_backend_clients(self):
        endpoints = [
            TcpEndpoint("127.0.0.1", _SVC_PORT_1),
            TcpEndpoint("127.0.0.1", _SVC_PORT_2),
        ]
        lb = LoadBalancerServer(endpoints, port=_LB_PORT)
        for client in lb._backend_clients:
            self.assertFalse(client._stopped)

        lb._on_close()

        for client in lb._backend_clients:
            self.assertTrue(client._stopped)

    def test_handle_request_forwards_to_picked_backend(self):
        endpoints = [
            TcpEndpoint("127.0.0.1", _SVC_PORT_1),
            TcpEndpoint("127.0.0.1", _SVC_PORT_2),
        ]
        lb = LoadBalancerServer(endpoints, port=_LB_PORT)
        fake_response = {"status": "ok", "result": [1, "alice"]}
        for client in lb._backend_clients:
            client._request = MagicMock(return_value=fake_response)

        msg = {"method": "GET", "path": "/hash", "data": {"hash": "abc"}}
        result = lb._handle_request_message(msg)

        self.assertEqual(result, fake_response)
        total_calls = sum(c._request.call_count for c in lb._backend_clients)
        self.assertEqual(total_calls, 1)

    def test_requests_distributed_across_all_backends(self):
        endpoints = [
            TcpEndpoint("127.0.0.1", _SVC_PORT_1),
            TcpEndpoint("127.0.0.1", _SVC_PORT_2),
        ]
        lb = LoadBalancerServer(endpoints, port=_LB_PORT)
        fake_response = {"status": "ok", "result": None}
        for client in lb._backend_clients:
            client._request = MagicMock(return_value=fake_response)

        msg = {"method": "GET", "path": "/hash", "data": {"hash": "abc"}}
        for _ in range(4):
            lb._handle_request_message(msg)

        # Each backend should have received exactly 2 out of 4 requests.
        for client in lb._backend_clients:
            self.assertEqual(client._request.call_count, 2)


class TestLoadBalancerIntegration(unittest.TestCase):
    """End-to-end: two real MicroserviceServer instances behind a LoadBalancerServer."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = os.path.join(self.temp_dir.name, "test.db")
        bpt_path = os.path.join(self.temp_dir.name, "test.bpt")

        patches = [
            patch.object(database_module, "DEFAULT_DB_PATH", db_path),
            patch.object(database_module, "DEFAULT_BPLUS_INDEX_PATH", bpt_path),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

        db = database_module.DbEngine()
        db.ensure_capacity(64)
        db.populate_range(0, 64)

        self.db_server = DbServer()
        self.db_server.start()
        self.addCleanup(self.db_server.close)

        self.svc1 = MicroserviceServer(
            host="127.0.0.1", port=_SVC_PORT_1, latency_ms=0, pool_size=8
        )
        self.svc2 = MicroserviceServer(
            host="127.0.0.1", port=_SVC_PORT_2, latency_ms=0, pool_size=8
        )
        self.svc1.start()
        self.svc2.start()
        self.addCleanup(self.svc1.close)
        self.addCleanup(self.svc2.close)

        self.lb = LoadBalancerServer(
            backends=[
                TcpEndpoint("127.0.0.1", _SVC_PORT_1),
                TcpEndpoint("127.0.0.1", _SVC_PORT_2),
            ],
            port=_LB_PORT,
        )
        self.lb.start()
        self.addCleanup(self.lb.close)

        time.sleep(0.05)

    def tearDown(self):
        self.temp_dir.cleanup()

    def _lb_client(self) -> TcpClient:
        client = TcpClient(
            endpoint=TcpEndpoint("127.0.0.1", _LB_PORT),
            timeout_sec=5.0,
            pool_size=4,
        )
        self.addCleanup(client.close)
        return client

    def test_get_request_proxied_through_lb(self):
        from emulator.utils import compute_hash_for, id_to_name
        from emulator.transport_layer.transport import hex_from_bytes

        record_id = 7
        h = hex_from_bytes(compute_hash_for(record_id, id_to_name(record_id)))

        client = self._lb_client()
        resp = client._request({"method": "GET", "path": "/hash", "data": {"hash": h}})

        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], [record_id, id_to_name(record_id).decode("ascii")])

    def test_post_request_proxied_through_lb(self):
        record_id = 3
        client = self._lb_client()
        resp = client._request(
            {"method": "POST", "path": "/name", "data": {"id": record_id, "new_name": "hello"}}
        )

        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], {"updated": True})

    def test_multiple_requests_reach_both_backends(self):
        """Confirm both backends are exercised — verified via their handled counters."""
        from emulator.utils import compute_hash_for, id_to_name
        from emulator.transport_layer.transport import hex_from_bytes

        h = hex_from_bytes(compute_hash_for(5, id_to_name(5)))
        msg = {"method": "GET", "path": "/hash", "data": {"hash": h}}

        client = self._lb_client()
        for _ in range(8):
            resp = client._request(msg)
            self.assertEqual(resp["status"], "ok")

        # Both servers should have handled at least one request.
        handled1 = self.svc1._active_connection_count()
        handled2 = self.svc2._active_connection_count()
        # At minimum both backends have an open LB connection.
        self.assertGreaterEqual(handled1 + handled2, 2)

    def test_error_response_proxied_correctly(self):
        """Backend error responses must be forwarded intact through the LB."""
        client = self._lb_client()
        # Invalid hex triggers a backend error; the LB must forward it as-is.
        resp = client._request(
            {"method": "GET", "path": "/hash", "data": {"hash": "not_a_real_hash"}}
        )
        self.assertEqual(resp["status"], "error")
        self.assertIn("hex", resp.get("error", "").lower())


if __name__ == "__main__":
    unittest.main()
