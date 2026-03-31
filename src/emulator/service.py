import threading
import time
import queue
from typing import Any, Dict

class Request:
    def __init__(self, op: str, payload: Dict, reply_q: queue.Queue):
        self.op = op
        self.payload = payload
        self.reply_q = reply_q

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
                results = list(self.db.query(req.payload.get("predicate", lambda x: True)))
                req.reply_q.put({"status": "ok", "results": results})
            else:
                req.reply_q.put({"status": "error", "error": "unknown op"})
            self.q.task_done()

    def send(self, req: Request):
        self.q.put(req)

    def stop(self):
        self._shutdown.set()
        self.thread.join(timeout=1)