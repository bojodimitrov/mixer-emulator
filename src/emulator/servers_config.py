"""Centralized host/port configuration for socket-based components.

Keep all defaults in one place so you can tweak endpoints without touching
each server/client implementation.

You can override these per constructor call, but most demos/tests should just
pull defaults from here.
"""

from __future__ import annotations

from dataclasses import dataclass

from .storage.constants import SHARD_COUNT


@dataclass(frozen=True)
class DbEndpoint:
    host: str = "127.0.0.1"
    port: int = 50001


@dataclass(frozen=True)
class ServiceEndpoint:
    host: str = "127.0.0.1"
    port: int = 50100


@dataclass(frozen=True)
class MetricsEndpoint:
    host: str = "127.0.0.1"
    port: int = 50003


@dataclass(frozen=True)
class CacheEndpoint:
    host: str = "127.0.0.1"
    port: int = 50004


@dataclass(frozen=True)
class LoadBalancerEndpoint:
    host: str = "127.0.0.1"
    port: int = 50002


DB_ENDPOINT = DbEndpoint()
GSI_ENDPOINT = DbEndpoint(host="127.0.0.1", port=50010)
DB_SHARD_ENDPOINTS = [
    DbEndpoint(host="127.0.0.1", port=50011 + i) for i in range(SHARD_COUNT)
]
SERVICE_ENDPOINTS = [
    ServiceEndpoint(port=50100),
    ServiceEndpoint(port=50101),
    ServiceEndpoint(port=50102),
    ServiceEndpoint(port=50103),
]
METRICS_ENDPOINT = MetricsEndpoint()
CACHE_ENDPOINT = CacheEndpoint()
LOAD_BALANCER_ENDPOINT = LoadBalancerEndpoint()
