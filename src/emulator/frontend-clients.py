import queue
import random
import string
import time
from typing import Any, Dict, List, Optional

from .service import Request
from .utils import compute_hash_for, id_to_name

_MAX_DB_ID = 11_881_375  # 26^5 - 1, last valid record id


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _random_name() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(5))


def _send_request(service, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    reply_q: queue.Queue = queue.Queue()
    req = Request(method, payload, reply_q)
    service.send(req)
    return reply_q.get()


def _sleep_pause(pause_ms: Optional[float], pause_range_ms: tuple) -> None:
    if pause_ms is None:
        pause_ms = random.uniform(pause_range_ms[0], pause_range_ms[1])
    time.sleep(max(0.0, float(pause_ms) / 1000.0))


# ---------------------------------------------------------------------------
# Corrupter
# ---------------------------------------------------------------------------


class Corrupter:
    """Simulates a frontend client that corrupts records by POSTing random names."""

    def __init__(
        self,
        service,
        planned_capacity: int = 200,
        pause_range_ms: tuple[float, float] = (150.0, 600.0),
    ):
        if planned_capacity <= 0:
            raise ValueError("planned_capacity must be positive")

        self.service = service
        self.planned_capacity = int(planned_capacity)
        self.pause_range_ms = pause_range_ms
        self._planned_requests: List[Dict[str, Any]] = []
        self._seed_random_plan()

    def _seed_random_plan(self) -> None:
        request_count = random.randint(1, self.planned_capacity)
        for _ in range(request_count):
            pause_ms = random.uniform(self.pause_range_ms[0], self.pause_range_ms[1])
            self._planned_requests.append(
                {
                    "payload": {
                        "id": random.randint(0, _MAX_DB_ID),
                        "new_name": _random_name(),
                    },
                    "pause_ms": pause_ms,
                }
            )

    def planned_count(self) -> int:
        return len(self._planned_requests)

    def run_planned_requests(self, request_count: Optional[int] = None) -> None:
        if not self._planned_requests:
            raise ValueError("no planned requests to run")

        plan = (
            self._planned_requests
            if request_count is None
            else self._planned_requests[:request_count]
        )

        for entry in plan:
            _send_request(self.service, "POST", entry["payload"])
            _sleep_pause(entry.get("pause_ms"), self.pause_range_ms)


# ---------------------------------------------------------------------------
# Repairer
# ---------------------------------------------------------------------------


class Repairer:
    """
    Simulates a frontend client that detects and repairs corrupted records.

    For each planned id:
    1. Derive the canonical name and hash via id_to_name / compute_hash_for.
    2. Send a GET for that hash.
    3. If found intact → record as "ok".
    4. If not found → the record was corrupted → POST the correct name back.
    """

    def __init__(
        self,
        service,
        planned_capacity: int = 200,
        pause_range_ms: tuple[float, float] = (150.0, 600.0),
    ):
        if planned_capacity <= 0:
            raise ValueError("planned_capacity must be positive")

        self.service = service
        self.planned_capacity = int(planned_capacity)
        self.pause_range_ms = pause_range_ms
        self._planned_requests: List[Dict[str, Any]] = []
        self._seed_random_plan()

    def _seed_random_plan(self) -> None:
        request_count = random.randint(1, self.planned_capacity)
        for _ in range(request_count):
            pause_ms = random.uniform(self.pause_range_ms[0], self.pause_range_ms[1])
            self._planned_requests.append(
                {
                    "id": random.randint(0, _MAX_DB_ID),
                    "pause_ms": pause_ms,
                }
            )

    def planned_count(self) -> int:
        return len(self._planned_requests)

    def run_planned_requests(
        self, request_count: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        if not self._planned_requests:
            raise ValueError("no planned requests to run")

        plan = (
            self._planned_requests
            if request_count is None
            else self._planned_requests[:request_count]
        )

        responses: List[Dict[str, Any]] = []
        for entry in plan:
            id_ = entry["id"]
            correct_name = id_to_name(id_)
            correct_hash = compute_hash_for(id_, correct_name)

            get_response = _send_request(self.service, "GET", {"hash": correct_hash})

            if (
                get_response.get("status") == "ok"
                and get_response.get("result") is not None
            ):
                responses.append({"action": "ok", "id": id_, "response": get_response})
            else:
                repair_response = _send_request(
                    self.service,
                    "POST",
                    {"id": id_, "new_name": correct_name.decode("ascii")},
                )
                responses.append(
                    {"action": "repaired", "id": id_, "response": repair_response}
                )

            _sleep_pause(entry.get("pause_ms"), self.pause_range_ms)

        return responses
