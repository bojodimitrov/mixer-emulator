import random
import string
import time
from typing import Any, Dict, List, Optional

from .socket_microservice import SocketMicroserviceClient
from .utils import compute_hash_for, id_to_name

_MAX_DB_ID = 11_881_375  # 26^5 - 1, last valid record id


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _random_name() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(5))


def _send_request(client: SocketMicroserviceClient, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    method_u = method.upper()
    if method_u == "GET":
        return client.get(payload["hash"])
    if method_u == "POST":
        return client.post(int(payload["id"]), str(payload["new_name"]))
    raise ValueError("method must be GET or POST")


# ---------------------------------------------------------------------------
# Public runners (for orchestration)
# ---------------------------------------------------------------------------


def run_single_request(
    *,
    kind: str,
    record_id: Optional[int] = None,
    new_name: Optional[str] = None,
    timeout_sec: float = 5.0,
) -> Dict[str, Any]:
    """Send exactly one request.

    Args:
        kind: "get" or "post" (case-insensitive)
        record_id: Optional explicit id; if omitted, a random one is chosen.
        new_name: For post requests; if omitted a random name is chosen.
        timeout_sec: TCP connect/read timeout.

    Returns:
        The response dict from the service.
    """

    kind_l = kind.strip().lower()
    if record_id is None:
        record_id = random.randint(0, _MAX_DB_ID)

    client = SocketMicroserviceClient(timeout_sec=float(timeout_sec))

    if kind_l == "get":
        correct_name = id_to_name(record_id)
        correct_hash = compute_hash_for(record_id, correct_name)
        return _send_request(client, "GET", {"hash": correct_hash})

    if kind_l == "post":
        if new_name is None:
            new_name = _random_name()
    return _send_request(client, "POST", {"id": int(record_id), "new_name": str(new_name)})

    raise ValueError("kind must be 'get' or 'post'")


def run_loop(
    *,
    kind: str,
    record_id: Optional[int] = None,
    new_name: Optional[str] = None,
    pause_ms: float = 0.0,
    seed: Optional[int] = None,
    timeout_sec: float = 5.0,
) -> None:
    """Run the same single-request action forever (until interrupted).

    This is intended for running frontend traffic in its own process so it can
    be started/stopped by an orchestrator (or manually with Ctrl+C).
    """

    if seed is not None:
        random.seed(int(seed))

    try:
        while True:
            run_single_request(
                kind=kind,
                record_id=record_id,
                new_name=new_name,
                timeout_sec=timeout_sec,
            )
            if pause_ms:
                time.sleep(max(0.0, float(pause_ms) / 1000.0))
    except KeyboardInterrupt:
        # Clean stop for interactive runs.
        return


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

        resp = run_single_request(
            kind="post",
            record_id=record_id,
            new_name=new_name,
            timeout_sec=self.client.timeout_sec,
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

        get_response = _send_request(self.client, "GET", {"hash": correct_hash})
        if get_response.get("status") == "ok" and get_response.get("result") is not None:
            return {"action": "ok", "id": record_id, "response": get_response}

        repair_response = _send_request(
            self.client,
            "POST",
            {"id": int(record_id), "new_name": correct_name.decode("ascii")},
        )
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


def main(argv: Optional[List[str]] = None) -> int:
    """CLI wrapper around the frontend client behaviors.

    Note: this expects an in-process `Microservice`-like object by default.
    For socket-based orchestration, we'll add a separate socket frontend runner.
    """

    import argparse

    parser = argparse.ArgumentParser(prog="frontend-clients")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_once = sub.add_parser("once", help="send exactly one request")
    p_once.add_argument(
        "--client",
        choices=["get", "corrupt", "repair"],
        required=True,
        help="which frontend behavior to run once",
    )
    p_once.add_argument("--id", dest="record_id", type=int, default=None)
    p_once.add_argument("--new-name", type=str, default=None)

    p_loop = sub.add_parser("loop", help="run indefinitely (Ctrl+C to stop)")
    p_loop.add_argument(
        "--client",
        choices=["get", "corrupt", "repair"],
        required=True,
        help="which frontend behavior to run in a loop",
    )
    p_loop.add_argument("--id", dest="record_id", type=int, default=None)
    p_loop.add_argument("--new-name", type=str, default=None)
    p_loop.add_argument("--pause-ms", type=float, default=0.0)
    p_loop.add_argument("--seed", type=int, default=None)

    args = parser.parse_args(argv)

    p_once.add_argument("--timeout-sec", type=float, default=5.0)

    p_loop.add_argument("--timeout-sec", type=float, default=5.0)

    # Socket-based runner (preferred).
    if args.cmd == "once":
        if args.client == "get":
            resp = run_single_request(
                kind="get",
                record_id=args.record_id,
                timeout_sec=args.timeout_sec,
            )
        elif args.client == "corrupt":
            resp = Corrupter(
                timeout_sec=args.timeout_sec,
            ).run_once(record_id=args.record_id, new_name=args.new_name)
        else:
            resp = Repairer(
                timeout_sec=args.timeout_sec,
            ).run_once(record_id=args.record_id)
        print(resp)
        return 0

    if args.client == "get":
        run_loop(
            kind="get",
            record_id=args.record_id,
            pause_ms=args.pause_ms,
            seed=args.seed,
            timeout_sec=args.timeout_sec,
        )
    elif args.client == "corrupt":
        Corrupter(
            timeout_sec=args.timeout_sec,
        ).run_loop(
            record_id=args.record_id,
            new_name=args.new_name,
            pause_ms=args.pause_ms,
            seed=args.seed,
        )
    else:
        Repairer(
            timeout_sec=args.timeout_sec,
        ).run_loop(
            record_id=args.record_id,
            pause_ms=args.pause_ms,
            seed=args.seed,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
