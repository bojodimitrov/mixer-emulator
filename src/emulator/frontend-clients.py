import random
import string
import time
from typing import Any, Dict, List, Optional

from .socket_microservice import SocketMicroserviceClient
from .transport import hex_from_bytes
from .utils import compute_hash_for, id_to_name

_MAX_DB_ID = 11_881_375  # 26^5 - 1, last valid record id

def _random_name() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(5))


class Corrupter:
    """Frontend client that sends POST requests to corrupt records."""

    def __init__(
        self,
        *,
        timeout_sec: float = 5.0,
    ):
        self.client = SocketMicroserviceClient(
            timeout_sec=float(timeout_sec),
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

        if record_id is None:
            record_id = random.randint(0, _MAX_DB_ID)
        if new_name is None:
            new_name = _random_name()

        resp = self.client.request(
            "POST",
            {"id": int(record_id), "new_name": str(new_name)},
        )

        # Include chosen inputs so orchestrators/demos can chain actions.
        if isinstance(resp, dict):
            resp = dict(resp)
            resp.setdefault("id", int(record_id))
            resp.setdefault("new_name", str(new_name))
        return resp

    def run_loop(
        self,
        *,
        record_id: Optional[int] = None,
        new_name: Optional[str] = None,
        pause_ms: float = 0.0,
        seed: Optional[int] = None,
    ) -> None:
        if seed is not None:
            random.seed(int(seed))
        try:
            while True:
                self.run_once(record_id=record_id, new_name=new_name)
                if pause_ms:
                    time.sleep(max(0.0, float(pause_ms) / 1000.0))
        except KeyboardInterrupt:
            return


class Repairer:
    """Frontend client that sends GET requests and, on missing records, repairs via POST."""

    def __init__(
        self,
        *,
        timeout_sec: float = 5.0,
    ):
        self.client = SocketMicroserviceClient(
            timeout_sec=float(timeout_sec),
        )

    def run_once(self, *, record_id: Optional[int] = None) -> Dict[str, Any]:
        """Attempt to read the canonical record, and repair if missing.

        Behavior:
          1) GET(correct_hash)
          2) If result is None -> POST(correct_name)
        """

        if record_id is None:
            record_id = random.randint(0, _MAX_DB_ID)

        correct_name = id_to_name(record_id)
        correct_hash = compute_hash_for(record_id, correct_name)

        get_response = self.client.request("GET", {"hash": hex_from_bytes(correct_hash)})
        if get_response.get("status") == "ok" and get_response.get("result") is not None:
            return {"action": "ok", "id": record_id, "response": get_response}

        repair_response = self.client.request("POST", {"id": int(record_id), "new_name": correct_name.decode("ascii")})
        
        return {"action": "repaired", "id": record_id, "response": repair_response}

    def run_loop(
        self,
        *,
        record_id: Optional[int] = None,
        pause_ms: float = 0.0,
        seed: Optional[int] = None,
    ) -> None:
        if seed is not None:
            random.seed(int(seed))
        try:
            while True:
                self.run_once(record_id=record_id)
                if pause_ms:
                    time.sleep(max(0.0, float(pause_ms) / 1000.0))
        except KeyboardInterrupt:
            return
