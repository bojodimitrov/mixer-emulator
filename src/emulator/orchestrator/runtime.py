from __future__ import annotations

import threading
from contextlib import suppress
from typing import List

from emulator.frontend.clients import Corrupter, Repairer
from emulator.microservice.server import MicroserviceServer
from emulator.storage.server import DbServer
from emulator.storage.orchestrator import LookupStrategy

from .metrics import RuntimeMetrics, now


class SystemOrchestrator:
    def __init__(
        self,
        *,
        db_lookup_strategy: LookupStrategy,
        corrupter_count: int,
        repairer_count: int,
        client_pause_ms: float,
    ) -> None:
        self.metrics = RuntimeMetrics()
        self._stop_event = threading.Event()
        self._threads: List[threading.Thread] = []

        self.db_server = DbServer(lookup_strategy=db_lookup_strategy)
        self.service_server = MicroserviceServer(latency_ms=15, pool_size=100)

        self._corrupter_count = max(0, int(corrupter_count))
        self._repairer_count = max(0, int(repairer_count))
        self._client_pause_ms = max(0.0, float(client_pause_ms))

        self._instrument_servers()

    @property
    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    def start(self) -> None:
        self.db_server.start()
        self.service_server.start()

        for idx in range(self._corrupter_count):
            t = threading.Thread(
                target=self._run_corrupter,
                name=f"corrupter-{idx}",
                daemon=True,
            )
            t.start()
            self._threads.append(t)

        for idx in range(self._repairer_count):
            t = threading.Thread(
                target=self._run_repairer,
                name=f"repairer-{idx}",
                daemon=True,
            )
            t.start()
            self._threads.append(t)

    def stop(self) -> None:
        if self._stop_event.is_set():
            return

        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=1.0)

        with suppress(Exception):
            self.service_server.close()
        with suppress(Exception):
            self.db_server.close()

    def _run_corrupter(self) -> None:
        client = Corrupter()
        run_once = client.run_once

        def measured_run_once(*, record_id=None, new_name=None):
            started = now()
            try:
                resp = run_once(record_id=record_id, new_name=new_name)
            except Exception as exc:
                self.metrics.record_client("corrupter", False, (now() - started) * 1000)
                return {"status": "error", "error": str(exc)}

            ok = isinstance(resp, dict) and resp.get("status") == "ok"
            self.metrics.record_client("corrupter", ok, (now() - started) * 1000)
            return resp

        client.run_once = measured_run_once  # type: ignore[method-assign]
        client.run_loop(pause_ms=self._client_pause_ms, cancel_token=self._stop_event)

    def _run_repairer(self) -> None:
        client = Repairer()
        run_once = client.run_once

        def measured_run_once(*, record_id=None):
            started = now()
            try:
                resp = run_once(record_id=record_id)
            except Exception as exc:
                self.metrics.record_client("repairer", False, (now() - started) * 1000)
                return {"status": "error", "error": str(exc)}

            ok = (
                isinstance(resp, dict)
                and resp.get("action") in {"ok", "repaired"}
                and isinstance(resp.get("response"), dict)
                and resp["response"].get("status") == "ok"
            )
            self.metrics.record_client("repairer", ok, (now() - started) * 1000)
            return resp

        client.run_once = measured_run_once  # type: ignore[method-assign]
        client.run_loop(pause_ms=self._client_pause_ms, cancel_token=self._stop_event)

    def _instrument_servers(self) -> None:
        original_db_dispatch = self.db_server._dispatch

        def wrapped_db_dispatch(req):
            started = now()
            try:
                resp = original_db_dispatch(req)
            except Exception:
                self.metrics.record_db(False, (now() - started) * 1000)
                raise

            ok = isinstance(resp, dict) and resp.get("status") == "ok"
            self.metrics.record_db(ok, (now() - started) * 1000)
            return resp

        self.db_server._dispatch = wrapped_db_dispatch  # type: ignore[method-assign]

        original_service_handle = self.service_server._handle_message

        def wrapped_service_handle(msg):
            started = now()
            try:
                resp = original_service_handle(msg)
            except Exception:
                self.metrics.record_service(False, (now() - started) * 1000)
                raise

            ok = isinstance(resp, dict) and resp.get("status") == "ok"
            self.metrics.record_service(ok, (now() - started) * 1000)
            return resp

        self.service_server._handle_message = wrapped_service_handle  # type: ignore[method-assign]
