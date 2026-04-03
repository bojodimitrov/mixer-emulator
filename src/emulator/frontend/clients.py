import random
import string
import time
from typing import Any, Dict, Optional

from emulator.frontend.loop_cancellation import LoopCancellation
from emulator.microservice.client import MicroserviceClient
from emulator.transport_layer.transport import hex_from_bytes
from emulator.utils import compute_hash_for, id_to_name

_MAX_DB_ID = 11_881_375  # 26^5 - 1, last valid record id


def _random_name() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(5))


class Corrupter:
    """Frontend client that sends POST requests to corrupt records."""

    def __init__(
        self,
        metrics_client: Any = None,
    ):
        # Each frontend worker runs on one thread, so one persistent socket is enough.
        self.client = MicroserviceClient(pool_size=1, retry_backoff_ms=5.0)
        self._metrics_client = metrics_client

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

            resp = self.client.request(
                "POST", {"id": int(record_id), "new_name": str(new_name)}, "/name"
            )

            ok = isinstance(resp, dict) and resp.get("status") == "ok"

            # Include chosen inputs so orchestrators/demos can chain actions.
            if isinstance(resp, dict):
                resp = dict(resp)
                resp.setdefault("id", int(record_id))
                resp.setdefault("new_name", str(new_name))
            return resp
        finally:
            if self._metrics_client is not None:
                self._metrics_client.record(
                    "corrupter", ok, (time.perf_counter() - started) * 1000.0
                )

    def run_loop(
        self,
        *,
        record_id: Optional[int] = None,
        new_name: Optional[str] = None,
        pause_ms: float = 0.0,
        seed: Optional[int] = None,
        cancel_token: Any = None,
    ) -> None:
        cancellation = LoopCancellation(cancel_token)
        if seed is not None:
            random.seed(int(seed))
        try:
            while not cancellation.is_cancelled():
                try:
                    self.run_once(record_id=record_id, new_name=new_name)
                except Exception as exc:
                    if self._metrics_client is not None:
                        self._metrics_client.record_error("corrupter", str(exc))
                    print(f"[corrupter] stopping after request failure: {exc}")
                    return
                if pause_ms:
                    if cancellation.pause_or_cancel(max(0.0, float(pause_ms) / 1000.0)):
                        break
        except KeyboardInterrupt:
            return
        finally:
            self.client.close()


class Repairer:
    """Frontend client that sends GET requests and, on missing records, repairs via POST."""

    def __init__(self, metrics_client: Any = None):
        # Keep one connection per worker to avoid excessive socket churn under load.
        self.client = MicroserviceClient(pool_size=1, retry_backoff_ms=5.0)
        self._metrics_client = metrics_client

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

            get_response = self.client.request(
                "GET", {"hash": hex_from_bytes(correct_hash)}, "/hash"
            )
            if (
                get_response.get("status") == "ok"
                and get_response.get("result") is not None
            ):
                ok = True
                return {"action": "ok", "id": record_id, "response": get_response}

            repair_response = self.client.request(
                "POST",
                {"id": int(record_id), "new_name": correct_name.decode("ascii")},
                "/name",
            )
            ok = (
                isinstance(repair_response, dict)
                and repair_response.get("status") == "ok"
            )
            return {"action": "repaired", "id": record_id, "response": repair_response}
        finally:
            if self._metrics_client is not None:
                self._metrics_client.record(
                    "repairer", ok, (time.perf_counter() - started) * 1000.0
                )

    def run_loop(
        self,
        *,
        record_id: Optional[int] = None,
        pause_ms: float = 0.0,
        seed: Optional[int] = None,
        cancel_token: Any = None,
    ) -> None:
        cancellation = LoopCancellation(cancel_token)
        if seed is not None:
            random.seed(int(seed))

        try:
            while not cancellation.is_cancelled():
                try:
                    self.run_once(record_id=record_id)
                except Exception as exc:
                    if self._metrics_client is not None:
                        self._metrics_client.record_error("repairer", str(exc))
                    print(f"[repairer] stopping after request failure: {exc}")
                    return
                if pause_ms:
                    if cancellation.pause_or_cancel(max(0.0, float(pause_ms) / 1000.0)):
                        break
        except KeyboardInterrupt:
            return
        finally:
            self.client.close()
