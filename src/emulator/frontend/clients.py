import random
import string
import time
from typing import Any, Callable, Dict, Optional, Union

from emulator.frontend.loop_cancellation import LoopCancellation
from emulator.microservice.client import MicroserviceClient
from emulator.transport_layer.transport import hex_from_bytes
from emulator.utils import compute_hash_for, id_to_name

_MAX_DB_ID = 11_881_375  # 26^5 - 1, last valid record id


def _random_name() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(5))


def _is_transient_service_pressure(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "retries exhausted" in text
        or "throttled by inflight gate" in text
        or "timed out" in text
    )


class FrontendClient:
    """Shared frontend client with axios-like request helpers."""

    service_name = "frontend"

    def __init__(
        self,
        metrics_client: Any = None,
        client: Optional[MicroserviceClient] = None,
    ):
        if client is None:
            self.client = MicroserviceClient(
                pool_size=1,
                retry_backoff_ms=5.0,
            )
            self._owns_client = True
        else:
            self.client = client
            self._owns_client = False
        self._metrics_client = metrics_client

    def request(
        self,
        config_or_method: Union[Dict[str, Any], str],
        data: Optional[Dict[str, Any]] = None,
        path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a request using either a config dict or method/data/path args."""

        if isinstance(config_or_method, dict):
            config = dict(config_or_method)
            method = str(config.pop("method", "GET")).upper()
            request_path = config.pop("url", config.pop("path", None))
            params = config.pop("params", None)
            body = config.pop("data", None)
            if config:
                unexpected = ", ".join(sorted(config.keys()))
                raise ValueError(f"unsupported request config fields: {unexpected}")
            if request_path is None:
                raise ValueError("request config requires 'url' or 'path'")
            request_data = params if params is not None else body
        else:
            method = str(config_or_method).upper()
            request_path = path
            request_data = data

        if request_path is None:
            raise ValueError("request path is required")

        return self.client.request(method, request_data or {}, request_path)

    def get(self, url: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.request({"method": "GET", "url": url, "params": params or {}})

    def post(self, url: str, *, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.request({"method": "POST", "url": url, "data": data or {}})

    def _record_metrics(self, ok: bool, started: float) -> None:
        if self._metrics_client is not None:
            self._metrics_client.record(
                self.service_name, ok, (time.perf_counter() - started) * 1000.0
            )

    def _run_loop(self, operation: Callable[[], Dict[str, Any]], *, pause_ms: float, seed: Optional[int], cancel_token: Any) -> None:
        cancellation = LoopCancellation(cancel_token)
        pending_transient_events = 0

        def _flush_transients() -> None:
            nonlocal pending_transient_events
            if pending_transient_events <= 0 or self._metrics_client is None:
                return
            self._metrics_client.record_transient(
                self.service_name,
                "frontend request pressure",
                pending_transient_events,
            )
            pending_transient_events = 0

        if seed is not None:
            random.seed(int(seed))

        try:
            while not cancellation.is_cancelled():
                try:
                    operation()
                    _flush_transients()
                except Exception as exc:
                    if _is_transient_service_pressure(exc):
                        pending_transient_events += 1
                        if pending_transient_events >= 25:
                            _flush_transients()
                        if cancellation.pause_or_cancel(0.05):
                            break
                        continue

                    _flush_transients()
                    if self._metrics_client is not None:
                        self._metrics_client.record_error(self.service_name, str(exc))
                    print(f"[{self.service_name}] stopping after request failure: {exc}")
                    return

                if pause_ms:
                    if cancellation.pause_or_cancel(max(0.0, float(pause_ms) / 1000.0)):
                        break
        except KeyboardInterrupt:
            return
        finally:
            _flush_transients()
            if self._owns_client:
                self.client.close()


class Corrupter(FrontendClient):
    """Frontend client that sends POST requests to corrupt records."""

    service_name = "corrupter"

    def __init__(
        self,
        metrics_client: Any = None,
        client: Optional[MicroserviceClient] = None,
    ):
        super().__init__(
            metrics_client=metrics_client,
            client=client,
        )

    def run_once(
        self,
        *,
        record_id: Optional[int] = None,
        new_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send one corruption POST.

        If record_id/new_name are omitted they are generated randomly.

        Returns the service response dict, enriched with the chosen id/name.
        """

        started = time.perf_counter()
        ok = False
        try:
            if record_id is None:
                record_id = random.randint(0, _MAX_DB_ID)
            if new_name is None:
                new_name = _random_name()

            correct_name = id_to_name(record_id)
            correct_hash = compute_hash_for(record_id, correct_name)

            get_response = self.get(
                "/hash", params={"hash": hex_from_bytes(correct_hash)}
            )
            if (
                get_response.get("status") == "ok"
                and get_response.get("result") is None
            ):
                ok = True
                return {"action": "ok", "id": record_id, "response": get_response}

            resp = self.post(
                "/name", data={"id": int(record_id), "new_name": str(new_name)}
            )

            ok = isinstance(resp, dict) and resp.get("status") == "ok"

            # Include chosen inputs so orchestrators/demos can chain actions.
            if isinstance(resp, dict):
                resp = dict(resp)
                resp.setdefault("id", int(record_id))
                resp.setdefault("new_name", str(new_name))
            return resp
        finally:
            self._record_metrics(ok, started)

    def run_loop(
        self,
        *,
        record_id: Optional[int] = None,
        new_name: Optional[str] = None,
        pause_ms: float = 0.0,
        seed: Optional[int] = None,
        cancel_token: Any = None,
    ) -> None:
        self._run_loop(
            lambda: self.run_once(record_id=record_id, new_name=new_name),
            pause_ms=pause_ms,
            seed=seed,
            cancel_token=cancel_token,
        )


class Repairer(FrontendClient):
    """Frontend client that sends GET requests and, on missing records, repairs via POST."""

    service_name = "repairer"

    def __init__(
        self,
        metrics_client: Any = None,
        client: Optional[MicroserviceClient] = None,
    ):
        super().__init__(
            metrics_client=metrics_client,
            client=client,
        )

    def run_once(self, *, record_id: Optional[int] = None) -> Dict[str, Any]:
        """Attempt to read the canonical record, and repair if missing.

        Behavior:
          1) GET(correct_hash)
          2) If result is None -> POST(correct_name)
        """

        started = time.perf_counter()
        ok = False
        try:
            if record_id is None:
                record_id = random.randint(0, _MAX_DB_ID)

            correct_name = id_to_name(record_id)
            correct_hash = compute_hash_for(record_id, correct_name)

            get_response = self.get(
                "/hash", params={"hash": hex_from_bytes(correct_hash)}
            )
            if (
                get_response.get("status") == "ok"
                and get_response.get("result") is not None
            ):
                ok = True
                return {"action": "ok", "id": record_id, "response": get_response}

            repair_response = self.post(
                "/name",
                data={"id": int(record_id), "new_name": correct_name.decode("ascii")},
            )
            ok = (
                isinstance(repair_response, dict)
                and repair_response.get("status") == "ok"
            )
            return {"action": "repaired", "id": record_id, "response": repair_response}
        finally:
            self._record_metrics(ok, started)

    def run_loop(
        self,
        *,
        record_id: Optional[int] = None,
        pause_ms: float = 0.0,
        seed: Optional[int] = None,
        cancel_token: Any = None,
    ) -> None:
        self._run_loop(
            lambda: self.run_once(record_id=record_id),
            pause_ms=pause_ms,
            seed=seed,
            cancel_token=cancel_token,
        )
