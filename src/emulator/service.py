import random
import time
import queue
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

from .storage.server import DatabaseRequest


class Request:
    def __init__(self, method: str, payload: Dict, reply_q: queue.Queue):
        self.method = method.upper()
        if self.method == "GET":
            if "hash" not in payload:
                raise ValueError("GET payload must include 'hash'")
            self.payload = {"hash": payload["hash"]}
        elif self.method == "POST":
            if "id" not in payload or "new_name" not in payload:
                raise ValueError("POST payload must include 'id' and 'new_name'")
            self.payload = {"id": payload["id"], "new_name": payload["new_name"]}
        else:
            raise ValueError("method must be GET or POST")
        self.reply_q = reply_q


class Microservice:
    def __init__(
        self,
        db_client,
        latency_ms: int = 50,
        pool_size: int = 500,
    ):
        if pool_size <= 0:
            raise ValueError("pool_size must be positive")

        self.db_client = db_client
        self.latency = max(0.0, latency_ms / 1000.0)
        self._executor = ThreadPoolExecutor(
            max_workers=int(pool_size),
            thread_name_prefix="microservice",
        )
        self._is_shutdown = False

    def _simulate_latency(self) -> None:
        jitter_sec = random.randint(-10, 10) / 1000.0
        delay = self.latency + jitter_sec
        time.sleep(max(0.0, delay))

    def _to_hash_bytes(self, hash_value):
        if isinstance(hash_value, (bytes, bytearray)):
            return bytes(hash_value)
        if isinstance(hash_value, str):
            return bytes.fromhex(hash_value)
        raise ValueError("GET payload must include hash as bytes or hex string")

    def _process_request(self, req: Request) -> None:
        try:
            self._simulate_latency()

            if req.method == "GET":
                result = self.db_client.query(self._to_hash_bytes(req.payload["hash"]))
                req.reply_q.put({"status": "ok", "result": result})
                return

            if req.method == "POST":
                id_ = req.payload["id"]
                new_name = req.payload["new_name"]
                updated = self.db_client.command(id_, new_name)
                req.reply_q.put({"status": "ok", "result": updated})

        except Exception as exc:
            req.reply_q.put({"status": "error", "error": str(exc)})

    def send(self, req: Request):
        if self._is_shutdown:
            req.reply_q.put({"status": "error", "error": "service is shut down"})
            return

        self._executor.submit(self._process_request, req)

    def stop(self):
        self._is_shutdown = True
        self._executor.shutdown(wait=True)


class DatabaseClient:
    def __init__(self, server):
        self.server = server

    def query(self, hash_bytes: bytes):
        req = DatabaseRequest("Query", {"hash_bytes": hash_bytes})
        return self.server.handle_request(req)

    def command(self, id_: int, new_name_str: str) -> bool:
        req = DatabaseRequest("Command", {"id": id_, "new_name": new_name_str})
        return self.server.handle_request(req)
