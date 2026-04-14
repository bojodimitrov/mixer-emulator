import unittest
import threading
from typing import Any

import importlib

from emulator.frontend.clients import Corrupter, Repairer
from emulator.microservice.server import MicroserviceServer
from emulator.storage.engine import DbEngine
from emulator.storage.server import DbServer
from tests.db_test_utils import create_seeded_temp_db


class _SocketHarness:
    def __init__(self):
        self.db_paths = None
        self.db_server = DbServer(
            lookup_strategy=DbEngine.STRATEGY_BPLUS,
        )
        self.svc_server = None

    def __enter__(self):
        # Seed a small temp DB so the socket stack returns OK responses.
        self.db_paths = create_seeded_temp_db(capacity=256, populate_end=256)

        self.db_server.start()
        self.svc_server = MicroserviceServer()
        self.svc_server.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.svc_server is not None:
            self.svc_server.close()
        self.db_server.close()
        if self.db_paths is not None:
            self.db_paths.temp_dir.cleanup()


class _FakeServiceClient:
    def __init__(self):
        self.calls = []

    def request(self, method, data, path):
        self.calls.append({"method": method, "data": data, "path": path})
        return {"status": "ok", "method": method, "data": data, "path": path}

    def close(self):
        return None


class _FakeCacheClient:
    def __init__(self):
        self.incr_calls = []

    def incr(self, key, amount=1):
        self.incr_calls.append({"key": key, "amount": amount})
        return amount


class TestFrontendClientsRunners(unittest.TestCase):
    def test_request_accepts_config_dict(self):
        fake_client: Any = _FakeServiceClient()

        resp = Corrupter(client=fake_client).request(
            {"method": "get", "url": "/hash", "params": {"hash": "abc"}}
        )

        self.assertEqual(resp["status"], "ok")
        self.assertEqual(
            fake_client.calls,
            [{"method": "GET", "data": {"hash": "abc"}, "path": "/hash"}],
        )

    def test_post_helper_matches_request_shape(self):
        fake_client: Any = _FakeServiceClient()

        resp = Repairer(client=fake_client).post(
            "/name", data={"id": 5, "new_name": "abcde"}
        )

        self.assertEqual(resp["status"], "ok")
        self.assertEqual(
            fake_client.calls,
            [
                {
                    "method": "POST",
                    "data": {"id": 5, "new_name": "abcde"},
                    "path": "/name",
                }
            ],
        )

    def test_corrupter_run_once_is_post(self):
        with _SocketHarness() as h:
            assert h.svc_server is not None
            resp = Corrupter().run_once(record_id=5, new_name="abcde")
            self.assertEqual(resp.get("id"), 5)
            if resp.get("status") == "ok":
                self.assertEqual(resp.get("new_name"), "abcde")
            else:
                self.assertEqual(resp.get("action"), "ok", msg=str(resp))
                self.assertEqual(resp.get("response", {}).get("status"), "ok")

    def test_corrupter_increments_counter_on_successful_post(self):
        fake_client: Any = _FakeServiceClient()
        fake_cache: Any = _FakeCacheClient()

        responses = iter(
            [
                {"status": "ok", "result": [5, "aaaaa"]},
                {"status": "ok", "result": {"updated": True}},
            ]
        )

        def _request(method, data, path):
            fake_client.calls.append({"method": method, "data": data, "path": path})
            return next(responses)

        fake_client.request = _request

        resp = Corrupter(client=fake_client, cache_client=fake_cache).run_once(
            record_id=5,
            new_name="abcde",
        )

        self.assertEqual(resp.get("status"), "ok")
        self.assertEqual(
            fake_cache.incr_calls,
            [{"key": "corrupted_rows", "amount": 1}],
        )

    def test_repairer_run_once_attempts_get_first(self):
        with _SocketHarness() as h:
            assert h.svc_server is not None
            resp = Repairer().run_once(record_id=5)
            self.assertIn(resp.get("action"), {"ok", "repaired"})

    def test_repairer_decrements_counter_on_successful_repair(self):
        fake_client: Any = _FakeServiceClient()
        fake_cache: Any = _FakeCacheClient()

        responses = iter(
            [
                {"status": "ok", "result": None},
                {"status": "ok", "result": {"updated": True}},
            ]
        )

        def _request(method, data, path):
            fake_client.calls.append({"method": method, "data": data, "path": path})
            return next(responses)

        fake_client.request = _request

        resp = Repairer(client=fake_client, cache_client=fake_cache).run_once(
            record_id=5,
        )

        self.assertEqual(resp.get("action"), "repaired")
        self.assertEqual(
            fake_cache.incr_calls,
            [{"key": "corrupted_rows", "amount": -1}],
        )

    def test_repairer_does_not_decrement_counter_on_noop_update(self):
        fake_client: Any = _FakeServiceClient()
        fake_cache: Any = _FakeCacheClient()

        responses = iter(
            [
                {"status": "ok", "result": None},
                {"status": "ok", "result": {"updated": False}},
            ]
        )

        def _request(method, data, path):
            fake_client.calls.append({"method": method, "data": data, "path": path})
            return next(responses)

        fake_client.request = _request

        resp = Repairer(client=fake_client, cache_client=fake_cache).run_once(record_id=5)

        self.assertEqual(resp.get("action"), "repaired")
        self.assertEqual(fake_cache.incr_calls, [])

    def test_corrupter_run_loop_stops_with_cancel_token(self):
        stop_event = threading.Event()
        call_count = {"n": 0}

        corrupter = Corrupter()

        def _fake_run_once(*, record_id=None, new_name=None):
            call_count["n"] += 1
            if call_count["n"] >= 2:
                stop_event.set()
            return {"status": "ok"}

        corrupter.run_once = _fake_run_once  # type: ignore[method-assign]
        corrupter.run_loop(cancel_token=stop_event)

        self.assertEqual(call_count["n"], 2)

    def test_repairer_run_loop_stops_with_cancel_token(self):
        stop_event = threading.Event()
        call_count = {"n": 0}

        repairer = Repairer()

        def _fake_run_once(*, record_id=None):
            call_count["n"] += 1
            if call_count["n"] >= 2:
                stop_event.set()
            return {"status": "ok"}

        repairer.run_once = _fake_run_once  # type: ignore[method-assign]
        repairer.run_loop(cancel_token=stop_event)

        self.assertEqual(call_count["n"], 2)


if __name__ == "__main__":
    unittest.main()
