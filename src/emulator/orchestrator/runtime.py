from __future__ import annotations

import threading
from contextlib import suppress
from typing import Dict, List

from emulator.cache.client import CacheClient
from emulator.cache.server import CacheServer
from emulator.frontend.clients import Corrupter, Repairer
from emulator.metrics.collector import MetricsCollectorClient, MetricsCollectorServer
from emulator.microservice.client import MicroserviceClient
from emulator.microservice.load_balancer import LoadBalancerServer
from emulator.microservice.server import MicroserviceServer
from emulator.servers_config import SERVICE_ENDPOINTS, GSI_ENDPOINT, DB_SHARD_ENDPOINTS
from emulator.storage.constants import SHARD_COUNT, db_shard_path
from emulator.storage.gsi_server import GsiServer
from emulator.storage.server import DbServer
from emulator.transport_layer.transport import TcpEndpoint

_CORRUPTED_ROWS_CACHE_KEY = "corrupted_rows"
_REPAIRED_ROWS_CACHE_KEY = "repaired_rows"


class SystemOrchestrator:
    def __init__(
        self,
        *,
        corrupter_count: int,
        repairer_count: int,
        client_pause_ms: float,
        ramp_up_step_sec: float = 0.0,
    ) -> None:
        self._stop_event = threading.Event()
        self._threads: List[threading.Thread] = []
        self._corrupter_threads: List[threading.Thread] = []
        self._repairer_threads: List[threading.Thread] = []
        self.metrics_server = MetricsCollectorServer()
        self.metrics = MetricsCollectorClient()
        self.cache_server = CacheServer()

        self._corrupter_count = max(0, int(corrupter_count))
        self._repairer_count = max(0, int(repairer_count))
        self._client_pause_ms = max(0.0, float(client_pause_ms))
        self._ramp_up_step_sec = max(0.0, float(ramp_up_step_sec))

        # Fixed request worker budget for service-side work. Keep independent from
        # frontend client thread counts to reflect production-style sizing.

        self.gsi_server = GsiServer(
            host=GSI_ENDPOINT.host,
            port=GSI_ENDPOINT.port,
        )
        self.db_shard_servers: List[DbServer] = [
            DbServer(
                host=DB_SHARD_ENDPOINTS[i].host,
                port=DB_SHARD_ENDPOINTS[i].port,
                db_path=db_shard_path(i),
                shard_index=i,
                shard_count=SHARD_COUNT,
            )
            for i in range(SHARD_COUNT)
        ]
        self.service_servers = [
            MicroserviceServer(host=ep.host, port=ep.port)
            for ep in SERVICE_ENDPOINTS
        ]
        self.load_balancer = LoadBalancerServer(
            backends=[
                TcpEndpoint(ep.host, ep.port) for ep in SERVICE_ENDPOINTS
            ]
        )
        # Share bounded microservice connection pools across worker threads so
        # high logical client counts do not translate to unbounded socket fan-out.
        self._corrupter_client = MicroserviceClient(
            pool_size=192,
            retry_backoff_ms=5.0,
        )
        self._repairer_client = MicroserviceClient(
            pool_size=192,
            retry_backoff_ms=5.0,
        )
        self._cache_client = CacheClient(pool_size=32, retry_backoff_ms=5.0)

    @property
    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    def get_corrupted_rows_count(self) -> int:
        value = self._cache_client.get(_CORRUPTED_ROWS_CACHE_KEY)
        if value is None:
            return 0
        if isinstance(value, bool) or not isinstance(value, int):
            raise RuntimeError("corrupted_rows cache value is not an integer")
        return value

    def get_repaired_rows_count(self) -> int:
        value = self._cache_client.get(_REPAIRED_ROWS_CACHE_KEY)
        if value is None:
            return 0
        if isinstance(value, bool) or not isinstance(value, int):
            raise RuntimeError("repaired_rows cache value is not an integer")
        return value

    @staticmethod
    def _count_alive_threads(threads: List[threading.Thread]) -> int:
        return sum(1 for thread in threads if thread.is_alive())

    def get_worker_instance_counts(self) -> Dict[str, Dict[str, int]]:
        return {
            "corrupter": {
                "configured": self._corrupter_count,
                "running": self._count_alive_threads(self._corrupter_threads),
            },
            "repairer": {
                "configured": self._repairer_count,
                "running": self._count_alive_threads(self._repairer_threads),
            },
        }

    def start(self) -> None:
        self.metrics_server.start()
        self.cache_server.start()
        self.gsi_server.start()
        for shard in self.db_shard_servers:
            shard.start()
        for srv in self.service_servers:
            srv.start()
        self.load_balancer.start()

        self._cache_client.set(_CORRUPTED_ROWS_CACHE_KEY, 0)
        self._cache_client.set(_REPAIRED_ROWS_CACHE_KEY, 0)

        if self._ramp_up_step_sec > 0:
            t = threading.Thread(
                target=self._ramp_up_workers,
                name="ramp-up",
                daemon=True,
            )
            t.start()
            self._threads.append(t)
        else:
            self._spawn_workers(self._corrupter_count, self._repairer_count)

    def _spawn_workers(self, corrupter_n: int, repairer_n: int) -> None:
        """Spawn exactly corrupter_n corrupters and repairer_n repairers."""
        base_c = len(self._corrupter_threads)
        for idx in range(corrupter_n):
            t = threading.Thread(
                target=self._run_corrupter,
                name=f"corrupter-{base_c + idx}",
                daemon=True,
            )
            t.start()
            self._corrupter_threads.append(t)
            self._threads.append(t)

        base_r = len(self._repairer_threads)
        for idx in range(repairer_n):
            t = threading.Thread(
                target=self._run_repairer,
                name=f"repairer-{base_r + idx}",
                daemon=True,
            )
            t.start()
            self._repairer_threads.append(t)
            self._threads.append(t)

    def _ramp_up_workers(self) -> None:
        """Gradually spawn workers in 10 steps of 10 % each."""
        _STEPS = 10
        spawned_c = 0
        spawned_r = 0
        for step in range(1, _STEPS + 1):
            if self._stop_event.is_set():
                return
            target_c = round(self._corrupter_count * step / _STEPS)
            target_r = round(self._repairer_count * step / _STEPS)
            add_c = target_c - spawned_c
            add_r = target_r - spawned_r
            if add_c > 0 or add_r > 0:
                self._spawn_workers(add_c, add_r)
                spawned_c = target_c
                spawned_r = target_r
            if step < _STEPS:
                self._stop_event.wait(timeout=self._ramp_up_step_sec)

    def stop(self) -> None:
        if self._stop_event.is_set():
            return

        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=1.0)

        with suppress(Exception):
            self.load_balancer.close()
        for srv in self.service_servers:
            with suppress(Exception):
                srv.close()
        for shard in self.db_shard_servers:
            with suppress(Exception):
                shard.close()
        with suppress(Exception):
            self.gsi_server.close()
        with suppress(Exception):
            self.metrics_server.close()
        with suppress(Exception):
            self.cache_server.close()
        with suppress(Exception):
            self._corrupter_client.close()
        with suppress(Exception):
            self._repairer_client.close()
        with suppress(Exception):
            self._cache_client.close()

    def _run_corrupter(self) -> None:
        client = Corrupter(
            metrics_client=self.metrics,
            client=self._corrupter_client,
            cache_client=self._cache_client,
        )
        client.run_loop(pause_ms=self._client_pause_ms, cancel_token=self._stop_event)

    def _run_repairer(self) -> None:
        client = Repairer(
            metrics_client=self.metrics,
            client=self._repairer_client,
            cache_client=self._cache_client,
        )
        client.run_loop(pause_ms=self._client_pause_ms, cancel_token=self._stop_event)
