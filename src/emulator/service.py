import threading
import time
import queue
from typing import Dict

from .storage.server import DatabaseRequest, DatabaseServer, LookupStrategy


class Request:
    def __init__(self, op: str, payload: Dict, reply_q: queue.Queue):
        self.op = op
        self.payload = payload
        self.reply_q = reply_q


# Microservice class:
#  - I want to receive request again, this time either GET or POST: if it is a GET, call the DatabaseClient Query method with the hash that will be in the GET request, if it is a POST then call the command method
# - I want to simulate latency with some jitter
# - I want to be able to serve many requests concurrently as the DatabaseServer


class Microservice:
    def __init__(self, db, latency_ms: int = 20):
        self.db = db
        self.latency = latency_ms / 1000.0
        self.q = queue.Queue()
        self._shutdown = threading.Event()
        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()

    def _worker(self):
        while not self._shutdown.is_set():
            try:
                req: Request = self.q.get(timeout=0.1)
            except queue.Empty:
                continue
            # simulate latency
            time.sleep(self.latency)
            if req.op == "insert":
                pos = self.db.insert(req.payload)
                req.reply_q.put({"status": "ok", "pos": pos})
            elif req.op == "query":
                results = list(
                    self.db.query(req.payload.get("predicate", lambda x: True))
                )
                req.reply_q.put({"status": "ok", "results": results})
            else:
                req.reply_q.put({"status": "error", "error": "unknown op"})
            self.q.task_done()

    def send(self, req: Request):
        self.q.put(req)

    def stop(self):
        self._shutdown.set()
        self.thread.join(timeout=1)


class DatabaseClient:
    def __init__(self, server):
        self.server = server

    def query(self, hash_bytes: bytes):
        req = DatabaseRequest("Query", {"hash_bytes": hash_bytes})
        return self.server.handle_request(req)

    def command(self, id_: int, new_name_str: str) -> bool:
        req = DatabaseRequest("Command", {"id": id_, "new_name": new_name_str})
        return self.server.handle_request(req)
