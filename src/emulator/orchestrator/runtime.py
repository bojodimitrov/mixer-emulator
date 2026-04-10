from __future__ import annotations

import threading
from contextlib import suppress
from typing import List

from emulator.frontend.clients import Corrupter, Repairer
from emulator.metrics.collector import MetricsCollectorClient, MetricsCollectorServer
from emulator.microservice.client import MicroserviceClient
from emulator.microservice.server import MicroserviceServer
from emulator.storage.server import DbServer
from emulator.storage.orchestrator import LookupStrategy


class SystemOrchestrator:
    def __init__(
        self,
        *,
        db_lookup_strategy: LookupStrategy,
        corrupter_count: int,
        repairer_count: int,
        client_pause_ms: float,
    ) -> None:
        self._stop_event = threading.Event()
        self._threads: List[threading.Thread] = []
        self.metrics_server = MetricsCollectorServer()
        self.metrics = MetricsCollectorClient()

        self._corrupter_count = max(0, int(corrupter_count))
        self._repairer_count = max(0, int(repairer_count))
        self._client_pause_ms = max(0.0, float(client_pause_ms))

        # Fixed request worker budget for service-side work. Keep independent from
        # frontend client thread counts to reflect production-style sizing.

        self.db_server = DbServer(
            lookup_strategy=db_lookup_strategy,
        )
        self.service_server = MicroserviceServer()
        # Share bounded microservice connection pools across worker threads so
        # high logical client counts do not translate to unbounded socket fan-out.
        self._corrupter_client = MicroserviceClient(
            pool_size=192,
            retry_backoff_ms=5.0,
            max_inflight_global=None,
            inflight_group="corrupter",
        )
        self._repairer_client = MicroserviceClient(
            pool_size=192,
            retry_backoff_ms=5.0,
            max_inflight_global=None,
            inflight_group="repairer",
        )

    @property
    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    def start(self) -> None:
        self.metrics_server.start()
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
        with suppress(Exception):
            self.metrics_server.close()
        with suppress(Exception):
            self._corrupter_client.close()
        with suppress(Exception):
            self._repairer_client.close()

    def _run_corrupter(self) -> None:
        client = Corrupter(metrics_client=self.metrics, client=self._corrupter_client)
        client.run_loop(pause_ms=self._client_pause_ms, cancel_token=self._stop_event)

    def _run_repairer(self) -> None:
        client = Repairer(metrics_client=self.metrics, client=self._repairer_client)
        client.run_loop(pause_ms=self._client_pause_ms, cancel_token=self._stop_event)
