import random
import threading
import time
import queue
from contextlib import suppress
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Iterable, List, Tuple, Type

from emulator.storage.client import DbClient
from emulator.storage.gsi_client import GsiClient
from emulator.cache.client import CacheClient
from emulator.servers_config import SHARD_COUNT, DB_SHARD_ENDPOINTS, GSI_ENDPOINT
from emulator.transport_layer.transport import TcpEndpoint


class Request:

    def __init__(
        self,
        method: str,
        payload: Dict,
        path: str,
        reply_q: queue.Queue,
    ):
        self.method = method.upper()
        self.path = path
        self.payload = payload
        self.reply_q = reply_q


class Api:

    def __init__(self):
        self._routes: Dict[Tuple[str, str], Callable[[Dict[str, Any]], Any]] = {}

    def get(self, path: str):
        return self.route("GET", path)

    def post(self, path: str):
        return self.route("POST", path)

    def register_routes(self) -> None:
        """Hook for subclasses to register routes during initialization."""
        return

    def _normalize_path(self, path: str) -> str:
        text = str(path or "/")
        return text if text.startswith("/") else f"/{text}"

    def add_route(
        self,
        method: str,
        path: str,
        handler: Callable[[Dict[str, Any]], Any],
    ) -> None:
        key = (str(method).upper(), self._normalize_path(path))
        self._routes[key] = handler

    def route(self, method: str, path: str):
        def decorator(handler: Callable[[Dict[str, Any]], Any]):
            self.add_route(method, path, handler)
            return handler

        return decorator


class Microservice:

    def __init__(self, latency_ms: int = 50, pool_size: int = 100):
        if pool_size <= 0:
            raise ValueError("pool_size must be positive")

        self.latency = max(0.0, latency_ms / 1000.0)
        self._executor = ThreadPoolExecutor(
            max_workers=int(pool_size),
            thread_name_prefix="microservice",
        )
        self._is_shutdown = False
        self._controllers: list[Api] = []
        self.api = self._build_discovered_api()

    @staticmethod
    def _iter_api_subclasses(base: Type[Api]) -> Iterable[Type[Api]]:
        for sub in base.__subclasses__():
            yield sub
            yield from Microservice._iter_api_subclasses(sub)

    def _build_discovered_api(self) -> Api:
        aggregate = Api()
        self._controllers.clear()

        for api_cls in self._iter_api_subclasses(Api):
            try:
                controller = api_cls()
            except TypeError:
                # Skip classes that require constructor args.
                continue

            if not hasattr(controller, "_routes"):
                Api.__init__(controller)

            controller.register_routes()
            self._controllers.append(controller)
            for key, handler in controller._routes.items():
                if key in aggregate._routes:
                    method, path = key
                    raise ValueError(f"duplicate route detected: {method} {path}")
                aggregate._routes[key] = handler

        return aggregate

    def add_route(
        self,
        method: str,
        path: str,
        handler: Callable[[Dict[str, Any]], Any],
    ) -> None:
        self.api.add_route(method, path, handler)

    def _simulate_latency(self) -> None:
        if self.latency > 0:
            jitter_sec = random.randint(-10, 10) / 1000.0
            delay = self.latency + jitter_sec
            time.sleep(max(0.0, delay))

    def handle(self, method: str, path: str, payload: Dict[str, Any]) -> Any:
        key = (str(method).upper(), self.api._normalize_path(path))
        handler = self.api._routes.get(key)

        if handler is None:
            if not any(m == key[0] for m, _ in self.api._routes.keys()):
                raise ValueError(f"unsupported method: {key[0]}")
            raise ValueError(f"unsupported route: {key[0]} {key[1]}")

        return handler(payload)

    def _process_request(self, req: Request) -> None:
        try:
            self._simulate_latency()

            result = self.handle(req.method, req.path, req.payload)
            req.reply_q.put({"status": "ok", "result": result})

        except Exception as exc:
            req.reply_q.put({"status": "error", "error": str(exc)})

    def process(self, request: Request):
        if self._is_shutdown:
            request.reply_q.put({"status": "error", "error": "service is shut down"})
            return

        self._executor.submit(self._process_request, request)

    def stop(self):
        self._is_shutdown = True
        self._executor.shutdown(wait=True)
        for controller in self._controllers:
            close_fn = getattr(controller, "close", None)
            if callable(close_fn):
                with suppress(Exception):
                    close_fn()


# Cache key prefixes used by the distributed hash read cache.
_CACHE_HASH_PREFIX = "hash:"
_CACHE_IDX_PREFIX = "hidx:"
# TTL for hash cache entries.
# Short enough that corruption/repair cycles are visible within a few seconds;
# long enough to collapse repeated identical hash lookups under high load.
_HASH_CACHE_TTL_SEC = 10.0


class CustomApi(Api):
    """GSI-backed sharded API.

    Read path (GET /hash):
      1. Check distributed cache (fast path).
      2. GsiClient.lookup(hash) → global_id.
      3. Route to shard[global_id % SHARD_COUNT].get_by_id(global_id).
      4. Cache result.

    Write path (POST /name):
      1. Route to shard via id % SHARD_COUNT.
      2. CommandWithHashes → returns {updated, old_hash, new_hash}.
      3. Evict distributed cache entry.
      4. Fire-and-forget thread → GsiClient.update() (eventual consistency).
    """

    def __init__(self) -> None:
        super().__init__()
        self._gsi = GsiClient(
            host=GSI_ENDPOINT.host,
            port=GSI_ENDPOINT.port,
            pool_size=8,
        )
        self._shards: List[DbClient] = [
            DbClient(
                endpoint=TcpEndpoint(DB_SHARD_ENDPOINTS[i].host, DB_SHARD_ENDPOINTS[i].port),
                pool_size=8,
                eager_connect=False,
                max_retries=3,
                retry_backoff_ms=50.0,
                max_idle_sec=45.0,
            )
            for i in range(SHARD_COUNT)
        ]
        self._cache = CacheClient(
            pool_size=8,
            max_idle_sec=45.0,
        )

    def close(self) -> None:
        with suppress(Exception):
            self._gsi.close()
        for shard in self._shards:
            with suppress(Exception):
                shard.close()
        with suppress(Exception):
            self._cache.close()

    def register_routes(self) -> None:
        self.get("/hash")(self.get_by_hash)
        self.post("/name")(self.post_by_id)

    # ── cache helpers ──────────────────────────────────────────────────────

    def _cache_get(self, key: str) -> Any:
        try:
            return self._cache.get(key)
        except Exception:
            return None

    def _cache_set(self, key: str, value: Any, ttl_sec: float) -> None:
        try:
            self._cache.set(key, value, ttl_sec=ttl_sec)
        except Exception:
            pass

    def _cache_delete(self, key: str) -> None:
        try:
            self._cache.delete(key)
        except Exception:
            pass

    # ── route handlers ─────────────────────────────────────────────────────

    def get_by_hash(self, payload: Dict[str, Any]) -> Any:
        hash_hex: str = payload["hash"]

        # 1. Cache hit fast path.
        cached = self._cache_get(_CACHE_HASH_PREFIX + hash_hex)
        if cached is not None:
            return cached

        # 2. GSI lookup: hash → global_id.
        global_id = self._gsi.lookup(hash_hex)
        if global_id is None:
            return None

        # 3. Fetch from the owning shard.
        result = self._shards[global_id % SHARD_COUNT].get_by_id(global_id)
        if result is None:
            return None

        # 4. Populate cache.
        id_, name = result
        self._cache_set(_CACHE_HASH_PREFIX + hash_hex, list(result), _HASH_CACHE_TTL_SEC)
        self._cache_set(_CACHE_IDX_PREFIX + str(id_), hash_hex, _HASH_CACHE_TTL_SEC)
        return result

    def post_by_id(self, payload: Dict[str, Any]) -> Dict[str, bool]:
        id_: int = payload["id"]
        new_name: str = payload["new_name"]

        info = self._shards[id_ % SHARD_COUNT].command_with_hashes(id_, new_name)
        updated: bool = bool(info.get("updated"))

        if updated:
            old_hash: str = info["old_hash"]
            new_hash: str = info["new_hash"]

            # Evict stale distributed cache entries.
            self._cache_delete(_CACHE_HASH_PREFIX + old_hash)
            self._cache_delete(_CACHE_IDX_PREFIX + str(id_))

            # Fire-and-forget GSI update (eventual consistency).
            gsi = self._gsi

            def _gsi_update() -> None:
                try:
                    gsi.update(old_hash, new_hash, id_)
                except Exception:
                    pass  # GSI is best-effort; shard is the source of truth

            threading.Thread(target=_gsi_update, daemon=True).start()

        return {"updated": updated}
