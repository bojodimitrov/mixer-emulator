import queue
from .service import Request
from .storage.server import DatabaseRequest


class Client:
    def __init__(self, service):
        self.service = service

    def insert(self, payload):
        reply_q = queue.Queue()
        req = Request("insert", payload, reply_q)
        self.service.send(req)
        return reply_q.get()  # blocking until reply

    def query(self, predicate):
        reply_q = queue.Queue()
        req = Request("query", {"predicate": predicate}, reply_q)
        self.service.send(req)
        return reply_q.get()
