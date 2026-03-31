# ...existing code...
import random
import time
import mmap, os

def run_app():
    # demo: start DB + service + client
    from .database import FileDB
    from .sorted_index import IndexBuilder
    from .service import Microservice
    from .client import Client

    db = FileDB()

    t0 = time.perf_counter()
    id_read, name, hashb = db.read_record(random.randint(0, db.record_count() - 1))
    t1 = time.perf_counter()
    print(f"Query took {(t1 - t0)*1000:.3f} ms")
    print(f"read_record({10_000_000}) -> id={id_read}, name={name}, hash={hashb.hex()}")

    t0 = time.perf_counter()
    h_bytes = bytes.fromhex(hashb.hex())
    result = db.query_by_hash(h_bytes)
    t1 = time.perf_counter()

    print(result)
    print(f"Query took {(t1 - t0)*1000:.3f} ms")

    # t0 = time.perf_counter()
    # h_bytes = bytes.fromhex(hashb.hex())
    # result = db.query_by_hash_index(h_bytes)
    # t1 = time.perf_counter()

    # print(result)
    # print(f"Query with index took {(t1 - t0)*1000:.3f} ms")


if __name__ == "__main__":
    run_app()
