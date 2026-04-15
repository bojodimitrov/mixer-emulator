import random
import time
import queue
from contextlib import suppress
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Iterable, Tuple, Type

from emulator.storage.client import DbClient
from emulator.cache.client import CacheClient


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
    """This where usually Web API frameworks custom code is built."""

    def __init__(self):
        super().__init__()
        # Service handlers are highly concurrent; keep a connection pool to avoid
        # creating a fresh outbound DB socket for every single request.
        # max_idle_sec stays below DbServer conn_timeout_sec (60s) so the pool
        # proactively discards sockets before the server closes them.
        self.db_client = DbClient(
            pool_size=32,
            eager_connect=False,
            max_retries=3,
            retry_backoff_ms=50.0,
            max_idle_sec=45.0,
        )
        # Distributed cache shared across all service instances.
        # Reads: "hash:{hex}" → [id, name] (None on miss, not cached).
        # Reverse index: "hidx:{id}" → hash_hex, used to evict on write.
        self._cache = CacheClient(
            pool_size=8,
            max_idle_sec=45.0,
        )

    def close(self) -> None:
        self.db_client.close()
        self._cache.close()

    def register_routes(self) -> None:
        self.get("/hash")(self.get_by_hash)
        self.post("/name")(self.post_by_id)

    def get_by_hash(self, payload: Dict[str, Any]):
        hash_hex = payload["hash"]
        try:
            cached = self._cache.get(_CACHE_HASH_PREFIX + hash_hex)
        except Exception:
            cached = None
        if cached is not None:
            return cached  # already a [id, name] list from JSON
        result = self.db_client.query(hash_hex)
        if result is not None:
            # Only cache hits; misses are fast in the B+ tree.
            id_, name = result
            try:
                self._cache.set(_CACHE_HASH_PREFIX + hash_hex, list(result), ttl_sec=_HASH_CACHE_TTL_SEC)
                self._cache.set(_CACHE_IDX_PREFIX + str(id_), hash_hex, ttl_sec=_HASH_CACHE_TTL_SEC)
            except Exception:
                pass  # cache is best-effort; never block a request
        return result

    @staticmethod
    def _build_update_response(updated: int) -> Dict[str, bool]:
        if isinstance(updated, bool) or not isinstance(updated, int):
            raise RuntimeError("db command result was not an integer")
        return {"updated": bool(updated)}

    def post_by_id(self, payload: Dict[str, Any]):
        id_ = payload["id"]
        new_name = payload["new_name"]
        result = self._build_update_response(self.db_client.command(id_, new_name))
        if result.get("updated"):
            # Evict stale hash entry via the reverse index.
            try:
                old_hash = self._cache.get(_CACHE_IDX_PREFIX + str(id_))
                if old_hash is not None:
                    self._cache.delete(_CACHE_HASH_PREFIX + str(old_hash))
                    self._cache.delete(_CACHE_IDX_PREFIX + str(id_))
            except Exception:
                pass  # cache is best-effort
        return result
