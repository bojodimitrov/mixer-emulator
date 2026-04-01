import random
import time
import queue
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Type

from emulator.storage.client import DbClient


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

    def __init__(self, latency_ms: int = 50, pool_size: int = 500):
        if pool_size <= 0:
            raise ValueError("pool_size must be positive")

        self.latency = max(0.0, latency_ms / 1000.0)
        self._executor = ThreadPoolExecutor(
            max_workers=int(pool_size),
            thread_name_prefix="microservice",
        )
        self._is_shutdown = False
        self.api = self._build_discovered_api()

    @staticmethod
    def _iter_api_subclasses(base: Type[Api]) -> Iterable[Type[Api]]:
        for sub in base.__subclasses__():
            yield sub
            yield from Microservice._iter_api_subclasses(sub)

    def _build_discovered_api(self) -> Api:
        aggregate = Api()

        for api_cls in self._iter_api_subclasses(Api):
            try:
                controller = api_cls()
            except TypeError:
                # Skip classes that require constructor args.
                continue

            if not hasattr(controller, "_routes"):
                Api.__init__(controller)

            controller.register_routes()
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
        jitter_sec = random.randint(-10, 10) / 1000.0
        delay = self.latency + jitter_sec
        time.sleep(max(0.0, delay))

    def _process_request(self, req: Request) -> None:
        try:
            # self._simulate_latency()

            key = (req.method, self.api._normalize_path(req.path))
            handler = self.api._routes.get(key)

            if handler is None:
                if not any(
                    method == req.method for method, _ in self.api._routes.keys()
                ):
                    raise ValueError(f"unsupported method: {req.method}")
                raise ValueError(f"unsupported route: {req.method} {key[1]}")

            result = handler(req.payload)
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


class CustomApi(Api):
    """This where usually Web API frameworks custom code is built."""

    def __init__(self):
        super().__init__()
        self.db_client = DbClient()

    def register_routes(self) -> None:
        self.get("/")(self.get_by_hash)
        self.post("/")(self.post_by_id)

    def get_by_hash(self, payload: Dict[str, Any]):
        return self.db_client.query(payload["hash"])

    def post_by_id(self, payload: Dict[str, Any]):
        id_ = payload["id"]
        new_name = payload["new_name"]
        return self.db_client.command(id_, new_name)
