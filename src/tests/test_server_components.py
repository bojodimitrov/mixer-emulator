import os
import socket
import struct
import tempfile
import time
import json
import threading
import unittest
from unittest.mock import patch

import emulator.storage.engine as database_module
from emulator.storage.client import DbClient
from emulator.storage.server import DbServer
from emulator.microservice.server import MicroserviceServer
from emulator.servers_config import DB_ENDPOINT


class _FakeSocket:
    def __init__(self):
        self._fileno = 123
        self.closed = False

    def fileno(self):
        return self._fileno

    def close(self):
        self.closed = True
        self._fileno = -1


class TestSocketDatabaseServerClient(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test.db")
        self.bpt_path = os.path.join(self.temp_dir.name, "test.bpt")

        patches = [
            patch.object(database_module, "DEFAULT_DB_PATH", self.db_path),
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
        h = compute_hash_for(record_id, name_b).hex()

        resp = client.query(h)
        self.assertEqual(resp, [record_id, name_b.decode("ascii")])

        result = client.command(record_id, "zzzzz")
        self.assertEqual(result, 1)

        # old hash should no longer match
        resp2 = client.query(h)
        self.assertIsNone(resp2)

    def test_pooled_close_is_graceful(self):
        client = DbClient(
            pool_size=1,
        )
        self.addCleanup(client.close)

        from emulator.utils import compute_hash_for, id_to_name

        record_id = 1
        name_b = id_to_name(record_id)
        h = compute_hash_for(record_id, name_b).hex()

        # Reuse one pooled connection and ensure close remains graceful.
        self.assertIsNotNone(client.query(h))

    def test_connection_pool_reuses_sockets(self):
        client = DbClient(
            pool_size=2,
        )
        self.addCleanup(client.close)

        from emulator.utils import compute_hash_for, id_to_name

        record_id = 2
        name_b = id_to_name(record_id)
        h = compute_hash_for(record_id, name_b).hex()

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
        h = compute_hash_for(record_id, name_b).hex()

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

    def test_server_returns_error_for_malformed_json_frame(self):
        from emulator.transport_layer.transport import recv_message, send_message

        with socket.create_connection(("127.0.0.1", self.db_port), timeout=1.0) as sock:
            sock.settimeout(1.0)

            bad_payload = b"{not-json"
            sock.sendall(struct.pack(">I", len(bad_payload)) + bad_payload)

            err = recv_message(sock)
            self.assertEqual(err.get("status"), "error")
            self.assertIn("invalid json payload", str(err.get("error", "")).lower())

            # Framing/protocol violations should terminate the connection.
            with self.assertRaises(ConnectionError):
                send_message(sock, {"op": "Close"})
                recv_message(sock)

    def test_server_returns_too_many_requests_when_pending_queue_overflows(self):
        from emulator.transport_layer.transport import recv_message
        from emulator.utils import compute_hash_for, id_to_name

        self.db_server.max_pending_requests_per_connection = 1
        record_id = 7
        query_msg = {
            "op": "Query",
            "hash": compute_hash_for(record_id, id_to_name(record_id)).hex(),
        }
        encoded = json.dumps(query_msg, separators=(",", ":")).encode("utf-8")
        frame = struct.pack(">I", len(encoded)) + encoded

        with socket.create_connection(("127.0.0.1", self.db_port), timeout=1.0) as sock:
            sock.settimeout(1.0)
            # Send many pipelined requests in one write to overflow the pending queue.
            sock.sendall(frame * 32)

            saw_overload = False
            deadline = time.time() + 2.0
            while time.time() < deadline:
                try:
                    resp = recv_message(sock)
                except (ConnectionError, socket.timeout):
                    break

                if (
                    resp.get("code") == 429
                    or "too many requests" in str(resp.get("error", "")).lower()
                ):
                    saw_overload = True
                    break

            self.assertTrue(saw_overload)

    def test_server_returns_too_many_requests_when_global_backlog_overflows(self):
        from emulator.transport_layer.transport import recv_message
        from emulator.utils import compute_hash_for, id_to_name

        self.db_server.max_pending_requests_per_connection = 32
        self.db_server.max_pending_requests_global = 1

        original_handle_request_message = self.db_server._handle_request_message
        release_request = threading.Event()

        def _delayed_handle_request_message(msg):
            release_request.wait(timeout=1.0)
            return original_handle_request_message(msg)

        self.db_server._handle_request_message = _delayed_handle_request_message  # type: ignore[method-assign]
        self.addCleanup(
            setattr,
            self.db_server,
            "_handle_request_message",
            original_handle_request_message,
        )

        record_id = 7
        query_msg = {
            "op": "Query",
            "hash": compute_hash_for(record_id, id_to_name(record_id)).hex(),
        }
        encoded = json.dumps(query_msg, separators=(",", ":")).encode("utf-8")
        frame = struct.pack(">I", len(encoded)) + encoded

        with socket.create_connection(("127.0.0.1", self.db_port), timeout=1.0) as sock:
            sock.settimeout(1.0)
            sock.sendall(frame * 2)

            overload = recv_message(sock)
            self.assertEqual(overload.get("code"), 429, msg=str(overload))
            self.assertIn(
                "server backlog limit exceeded",
                str(overload.get("detail", "")).lower(),
            )

            release_request.set()


class TestDbClientPoolAging(unittest.TestCase):
    def test_pool_recycles_socket_past_idle_timeout(self):
        client = DbClient(pool_size=1, eager_connect=False, max_idle_sec=0.01)
        self.addCleanup(client.close)

        stale = _FakeSocket()
        fresh = _FakeSocket()
        assert client._pool is not None

        now = time.monotonic()
        client._created = 1
        client._socket_timestamps[id(stale)] = (now - 1.0, now - 0.1)
        client._pool.put_nowait(stale)  # type: ignore[arg-type]

        with patch("emulator.storage.client.TcpEndpoint.connect", return_value=fresh):
            got = client._pool_acquire()

        self.assertIs(got, fresh)
        self.assertTrue(stale.closed)
        self.assertEqual(client._created, 1)

    def test_pool_recycles_socket_past_lifetime(self):
        client = DbClient(pool_size=1, eager_connect=False, max_lifetime_sec=0.01)
        self.addCleanup(client.close)

        stale = _FakeSocket()
        fresh = _FakeSocket()
        assert client._pool is not None

        now = time.monotonic()
        client._created = 1
        client._socket_timestamps[id(stale)] = (now - 0.1, now)
        client._pool.put_nowait(stale)  # type: ignore[arg-type]

        with patch("emulator.storage.client.TcpEndpoint.connect", return_value=fresh):
            got = client._pool_acquire()

        self.assertIs(got, fresh)
        self.assertTrue(stale.closed)
        self.assertEqual(client._created, 1)


class TestSocketMicroserviceErrorPaths(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test.db")
        self.bpt_path = os.path.join(self.temp_dir.name, "test.bpt")

        patches = [
            patch.object(database_module, "DEFAULT_DB_PATH", self.db_path),
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

    def test_close_drops_pending_requests_after_current_inflight(self):
        from emulator.transport_layer.transport import recv_message, send_message
        from emulator.utils import compute_hash_for, id_to_name

        assert self.db_server._socket is not None
        db_port = int(self.db_server._socket.getsockname()[1])

        record_id = 7
        query_hash = compute_hash_for(record_id, id_to_name(record_id)).hex()

        with socket.create_connection(("127.0.0.1", db_port), timeout=1.0) as sock:
            sock.settimeout(1.0)
            # Queue two normal requests followed by Close.
            send_message(sock, {"op": "Query", "hash": query_hash})
            send_message(sock, {"op": "Query", "hash": query_hash})
            send_message(sock, {"op": "Close"})

            first = recv_message(sock)
            self.assertEqual(first.get("status"), "ok")
            close_reply = recv_message(sock)
            self.assertEqual(close_reply.get("status"), "ok")
            self.assertTrue(close_reply.get("result"))

            # Connection should be closed before a second query response appears.
            with self.assertRaises(ConnectionError):
                recv_message(sock)


if __name__ == "__main__":
    unittest.main()
