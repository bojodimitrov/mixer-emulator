# Microservices Emulator

## Overview

This repository is a Python data-system emulator built on a local file database, hash indexes, and TCP service layers.

It includes:

- A mmap-backed file database with cross-process read/write locking
- Two lookup strategies: linear scan and B+ tree index
- In-process orchestration (`DbOrchestrator`) plus socket server/client layers (`DbServer`, `DbClient`)
- A microservice layer (`MicroserviceServer`, `MicroserviceClient`) and frontend runners for corruption/repair flows
- A round-robin TCP load balancer (`LoadBalancerServer`) in front of multiple microservice instances
- A Redis-like in-memory cache server (`CacheServer`, `CacheClient`) with optional per-key TTL
- An async metrics collection layer (`MetricsCollectorServer`, `MetricsCollectorClient`)
- A `FrontendClient` base class with axios-like request helpers shared by `Corrupter` and `Repairer`
- Writable B+ tree update path with rollback on index update failure

## Problems that forced evolution

Database Linear search -> sorted index

Update, insert, delete and maintainability of index -> B+ tree index

Scalability of database -> thread executor

Cold starts -> thread pool ready to receive requests

Multiple threads updating DB files -> Row-level locking and page level locking

Connection churn (Only one usage of each socket address... is normally permitted) -> Reactor pattern, readiness dispatch of requests on separate threads, event loop, keep-alive connections, persisten connection reuse
Pooled sockets are reused without checking if they are still healthy:
- ConnectionAbortedError: [WinError 10053] An established connection was aborted by the software in your host machine

Database bottleneck -> sharding by ID and GSI server for the hash values

Next improvement: upgrade the metrics with things like p95 and p99 latency, etc in order to find bottlenecks

## Project Structure

```
.
├── db/
│   ├── mixer_emulator_bin.db
│   ├── mixer_emulator.bpt
│   └── tmp_btree/
├── src/
│   ├── emulator/
│   │   ├── __init__.py
│   │   ├── main.py
│   │   ├── servers_config.py
│   │   ├── utils.py
│   │   ├── cache/
│   │   │   ├── client.py
│   │   │   ├── server.py
│   │   │   └── store.py
│   │   ├── frontend/
│   │   │   ├── clients.py
│   │   │   └── loop_cancellation.py
│   │   ├── metrics/
│   │   │   ├── collector.py
│   │   │   ├── corruption.py
│   │   │   └── runtime_metrics.py
│   │   ├── microservice/
│   │   │   ├── client.py
│   │   │   ├── framework.py
│   │   │   ├── load_balancer.py
│   │   │   └── server.py
│   │   ├── orchestrator/
│   │   │   ├── monitor.py
│   │   │   └── runtime.py
│   │   ├── demonstrations/
│   │   ├── storage/
│   │   │   ├── ...
│   │   └── transport_layer/
│   └── tests/
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Storage Layout

The project uses a src layout. Install it in editable mode or set `PYTHONPATH=src` before running modules directly.

Default generated files:

- `db/shard_0/records.db` … `db/shard_3/records.db`: sharded record files (each shard holds records where `global_id % 4 == shard_index`)
- `db/gsi/hash_index.bpt`: Global Secondary Index — flat B+ tree mapping `hash → global_id` across all shards
- `db/gsi/tmp_btree/`: temporary chunk files used while building the GSI

## Installation

Install dependencies:

```bash
pip install -r requirements.txt
```

Install in editable mode:

```bash
pip install -e .
```

Alternative without editable install (bash/zsh):

```bash
export PYTHONPATH=src
```

On Windows PowerShell:

```powershell
$env:PYTHONPATH = "src"
```

## Usage

Build the sharded database (4 shard files written in parallel):

```bash
python -m emulator.storage.database
```

Build a smaller database slice for quick local testing:

```bash
python -m emulator.storage.database --start 0 --end 100000
```

Build the GSI (reads all shard files, writes `db/gsi/hash_index.bpt`):

```bash
python -m emulator.storage.b_tree_index
```

Run the system orchestrator (starts DB server, microservice server, and frontend workers):

```bash
python -m emulator.main
```

The orchestrator opens a small metrics window and runs until you close that window.

Run headless (metrics in terminal):

```bash
python -m emulator.main --headless
```

Run headless for a fixed duration:

```bash
python -m emulator.main --headless --duration-sec 30
```

Tune worker counts and pacing:

```bash
python -m emulator.main --corrupters 4 --repairers 2 --client-pause-ms 10
```

Main orchestration flags:

- `--corrupters <int>`: number of corrupter client workers (default: `2`)
- `--repairers <int>`: number of repairer client workers (default: `2`)
- `--client-pause-ms <float>`: pause between client operations (default: `500.0`)
- `--ramp-up-step-sec <float>`: seconds between each 20% ramp-up step; `0` disables gradual ramp-up (default: `0.0`)
- `--headless`: print metrics in terminal instead of opening a window
- `--duration-sec <float>`: optional stop-after duration for headless runs

The old database demo still exists under `emulator.demonstrations`.

## Orchestrator Throughput History

Command used:

```bash
.venv/Scripts/python.exe -m emulator.main --duration-sec 30 --corrupters <n> --repairers <n>
```

Observed result:

- With `6` corrupters and `24` repairers, the system was fairly stable.
- After the row/page-level locking update, the system sustained high-concurrency runs with `200` corrupters and `200` repairers without recorded errors in the sampled 60.2s window.

| Duration (s) | Corrupters | Repairers | DB ops/s | Service ops/s | Total errors |
| ------------ | ---------: | --------: | -------: | ------------: | -----------: |
| 30           |          6 |        24 |    56.49 |         56.49 |  3+1 layered |
| 30           |          0 |        50 |   139.78 |        139.78 |            0 |
| 30           |         15 |         0 |    16.24 |         16.24 |    5 layered |
| 60           |        200 |       200 |   130.00 |        130.00 |            0 |

Quick read for first row:

- Average sampled throughput was about `56.49 ops/s` across DB and microservice layers.
- Repair traffic averaged about `44.30 ops/s`.
- Corruption traffic averaged about `12.09 ops/s`.
- End-of-run cumulative throughput reached `58.22 ops/s` for DB/service.
- Errors were low but non-zero: `3` at DB/service level and `1` on corrupter requests.

Notes:

- Wait time between frontend clients requests is 100ms.
- `Total errors` is currently recorded as layered counters, not a single end-to-end deduplicated value.
- For the 60.2s run with `200/200` workers, frontend cumulative averages were `66.44 ops/s` (corrupter) and `62.67 ops/s` (repairer), with zero recorded errors.
- Bottlenecks under high worker counts are now dominated by capacity and queueing/backpressure rather than full-table write locking.

## Server and Service Layers

The repository has two request-handling layers.

### `DbOrchestrator`

`emulator.storage.orchestrator.DbOrchestrator` is the main in-process concurrent execution layer. It uses a fixed-size thread pool and a shared `DbEngine` instance to process requests.

Supported request types:

- `Query`: resolve a `(hash -> (id, name))` lookup
- `Command`: update an existing record name

Example:

```python
from emulator.storage.orchestrator import DbRequest, DbOrchestrator
from emulator.storage.engine import DbEngine

server = DbOrchestrator(lookup_strategy=DbEngine.STRATEGY_BPLUS)
result = server.handle_request(DbRequest("Query", {"hash_bytes": some_hash}))
updated = server.handle_request(DbRequest("Command", {"id": 42, "new_name": "zzzzz"}))
server.close()
```

### `Microservice` / `MicroserviceClient`

`emulator.microservice.framework.Microservice` and `emulator.microservice.client.MicroserviceClient` provide a lightweight service wrapper that can simulate latency and expose GET/POST semantics over TCP.

### `LoadBalancerServer`

`emulator.microservice.load_balancer.LoadBalancerServer` is a round-robin TCP load balancer that distributes incoming requests across multiple microservice instances. It is intentionally decoupled from the microservice lifecycle — it only holds backend addresses and forwards requests; callers are responsible for starting/stopping the backends.

Default listen address: `127.0.0.1:50002` (overridable via constructor).

```python
from emulator.microservice.load_balancer import LoadBalancerServer
from emulator.transport_layer.transport import TcpEndpoint

lb = LoadBalancerServer(
    backends=[TcpEndpoint("127.0.0.1", 50100), TcpEndpoint("127.0.0.1", 50101)]
)
lb.start()
# ... lb.close()
```

### `CacheServer` / `CacheClient`

`emulator.cache.server.CacheServer` is a small Redis-like TCP key/value store backed by `CacheStore`, a thread-safe in-memory dict with optional per-key TTL.

Default listen address: `127.0.0.1:50004`.

Supported operations via `CacheClient`:

| Operation | Description |
| --------- | ----------- |
| `ping()` | Health check |
| `get(key)` | Retrieve a value (`None` if absent or expired) |
| `exists(key)` | Check key presence |
| `mget(keys)` | Bulk get, preserving order |
| `set(key, value, *, ttl_sec=None)` | Store with optional expiry |
| `incr(key, amount=1)` | Atomic integer increment (auto-initialises to 0) |
| `delete(key)` | Remove a key |
| `flush()` | Clear all keys |

Example:

```python
from emulator.cache.server import CacheServer
from emulator.cache.client import CacheClient

server = CacheServer()
server.start()

client = CacheClient()
client.set("hits", 0)
client.incr("hits", 5)
print(client.get("hits"))  # 5
client.set("session", {"user": "alice"}, ttl_sec=30.0)

server.close()
```

The `SystemOrchestrator` uses the cache to track live `corrupted_rows` and `repaired_rows` counters, which are displayed in the headless monitor output.

### `MetricsCollectorServer` / `MetricsCollectorClient`

`emulator.metrics.collector.MetricsCollectorServer` collects per-service latency, error, and transient-pressure events over TCP. `MetricsCollectorClient` batches outgoing records on a background sender thread to avoid blocking the hot path.

Default listen address: `127.0.0.1:50003`.

### `FrontendClient` / `Corrupter` / `Repairer`

`emulator.frontend.clients.FrontendClient` is a shared base class with an axios-like `request()` helper. It accepts either a config dict (`{"method": "POST", "url": "/name", "data": {...}}`) or positional `(method, data, path)` arguments, and delegates to a pooled `MicroserviceClient`.

`Corrupter` and `Repairer` extend `FrontendClient`:

- **`Corrupter.run_once()`** — picks a random record, checks its hash via `GET /hash`, and overwrites the name via `POST /name` to simulate corruption.
- **`Repairer.run_once()`** — picks a random record, checks via `GET /hash`, and restores the canonical name via `POST /name` if the record is missing or wrong.

Both support a `run_loop()` that repeats the operation with configurable pause and cancellation token.

## Socket-based Decoupling (DB -> Service -> Frontend)

In addition to the in-process queue-based components, the repo includes a small JSON-over-TCP layer that lets you run the database, microservice, and frontend as separate processes.

Key modules:

- `emulator/transport_layer/transport.py`: length-prefixed JSON framing (`send_message` / `recv_message`)
- `emulator/storage/server.py`: `DbServer` (TCP wrapper around `DbOrchestrator`)
- `emulator/storage/client.py`: `DbClient`
- `emulator/microservice/server.py`: `MicroserviceServer`
- `emulator/microservice/client.py`: `MicroserviceClient`
- `emulator/microservice/load_balancer.py`: `LoadBalancerServer`
- `emulator/cache/server.py`: `CacheServer`
- `emulator/cache/client.py`: `CacheClient`
- `emulator/metrics/collector.py`: `MetricsCollectorServer` / `MetricsCollectorClient`
- `emulator/servers_config.py`: centralised endpoint defaults

Default endpoint assignments:

| Component | Address |
| --------- | ------- |
| `GsiServer` | `127.0.0.1:50010` |
| `DbServer` shard 0 | `127.0.0.1:50011` |
| `DbServer` shard 1 | `127.0.0.1:50012` |
| `DbServer` shard 2 | `127.0.0.1:50013` |
| `DbServer` shard 3 | `127.0.0.1:50014` |
| `LoadBalancerServer` | `127.0.0.1:50002` |
| `MetricsCollectorServer` | `127.0.0.1:50003` |
| `CacheServer` | `127.0.0.1:50004` |
| `MicroserviceServer` instances | `127.0.0.1:50100–50103` |

### Architecture diagram

End-to-end request flow through the sharded runtime.

```text
 Frontend Workers
 ┌──────────────┐  ┌──────────────┐
 │  Corrupter   │  │   Repairer   │   (N threads each)
 └──────┬───────┘  └──────┬───────┘
        │                 │
        │  GET /hash  POST /name
        ▼                 ▼
 ┌────────────────────────────────┐
 │       LoadBalancerServer       │  127.0.0.1:50002
 │  round-robin across instances  │
 └───────────────┬────────────────┘
                 │
       ┌─────────┴──────────┐
       ▼                    ▼
 ┌───────────┐       ┌───────────┐
 │Microservice│  ...  │Microservice│  :50100 – :50103
 │  Server   │       │  Server   │
 └─────┬─────┘       └─────┬─────┘
       │  CustomApi handles each request:
       │
       │  GET /hash                    POST /name
       │  ┌─────────────────────┐      ┌──────────────────────────┐
       │  │ 1. CacheClient.get  │      │ 1. DbClient[id%4].        │
       │  │    (hash: prefix)   │      │    command_with_hashes()  │
       │  │ 2. GsiClient.lookup │      │ 2. CacheClient.delete     │
       │  │    hash → global_id │      │    (old_hash cache entry) │
       │  │ 3. DbClient[id%4]   │      │ 3. GsiClient.update()     │
       │  │    .get_by_id()     │      │    (fire-and-forget)      │
       │  │ 4. CacheClient.set  │      └──────────────┬───────────┘
       │  └──────────┬──────────┘                     │
       │             │                                 │
       ▼             ▼                                 ▼
 ┌───────────┐  ┌───────────┐              ┌────────────────────┐
 │CacheServer│  │ GsiServer │  :50010      │    GsiServer       │  :50010
 │  :50004   │  │           │              │  (same instance)   │
 │ key/value │  │BPlusTree  │              │  update(old→new)   │
 │ TTL store │  │hash→id    │              └────────┬───────────┘
 └───────────┘  └─────┬─────┘                       │
                      │ global_id                   │
                      ▼                             ▼
             ┌─────────────────────────────────────────────┐
             │             DbServer shards                  │
             │                                             │
             │  shard 0 :50011   shard 1 :50012            │
             │  shard 2 :50013   shard 3 :50014            │
             │                                             │
             │  routed by: global_id % 4                   │
             └──────────────────┬──────────────────────────┘
                                │
                     ┌──────────┴──────────┐
                     ▼                     ▼
              ┌────────────┐       ┌────────────┐
              │ db/shard_0 │  ...  │ db/shard_3 │
              │ records.db │       │ records.db │
              └────────────┘       └────────────┘

      db/gsi/hash_index.bpt  (flat B+ tree across all shards)
      built offline by: python -m emulator.storage.b_tree_index

 Supporting services (always running):
  MetricsCollectorServer  :50003   (latency, error, transient counters)
  CacheServer             :50004   (hash→record TTL cache)
```

#### TCP server internals (`TcpServerBase`)

This shows the request flow inside any server (DbServer, GsiServer, etc.).

```text
Caller thread
  |
  |  start()
  v
+---------------------------+
| TcpServerBase             |
|  - _sock (listening TCP)  |
|  - _stop_event            |
|  - _executor (threadpool) |
|  - _thread (accept loop)  |
+---------------------------+
       |
       | creates
       v
   +-------------------+
   | _thread           |   (1 background thread)
   | target = _serve() |
   +-------------------+
       |
       | loop: accept() with timeout
       v
   +-------------------+
   | new TCP conn      |
   +-------------------+
       |
       | submit to thread pool
       v
+----------------------------------+
| _executor: ThreadPoolExecutor    |  (N worker threads)
+----------------------------------+
       |
       v
   +--------------------------+
   | _handle_conn(conn)       |
   |  - TCP_NODELAY           |
   |  - keep-alive loop:      |
   |      recv_message()      |
   |      if op=="Close":     |
   |         send ok; return  |
   |      resp=_dispatch(req) |
   |      send_message(resp)  |
   +--------------------------+
       |
       v
     client disconnects
     or timeout / Close
```

## Running Tests

Run tests:

```bash
python -m pytest
```

Current test coverage includes:

- Core database read and linear-hash lookup behavior
- Concurrent reads across multiple `DbEngine` connections
- B+ tree build, lookup, insert, delete, update, and small-capacity regression cases
- `CacheServer` round-trip: Ping, Set, Get, Exists, MGet, Incr, Delete, TTL expiry

The suite currently includes server, client, transport, storage, cache, load balancer, and frontend client tests.

## Benchmark Snapshot

Latest run shape: `Repair (clean) -> Corrupt -> Repair (corrupted)`, 10 IDs per phase.

| Strategy               | Avg repair clean (ms) | Avg corrupt (ms) | Avg repair corrupted (ms) |
| ---------------------- | --------------------: | ---------------: | ------------------------: |
| No index (linear scan) |                800.82 |             7.33 |                   2719.60 |
| Sorted index           |                  5.97 |           929.77 |                   1104.98 |
| B+ tree                |                 18.91 |            38.38 |                     23.38 |

- `Repair clean` will execute only reads
- `Corrupt will` execute only writes
- `Repair corrupted` will execute both reads and writes

## File Formats

### Database File (.db)

Sequential fixed-size records.

```
Offset  Size   Field
──────────────────────────────────────
0       8      Record ID (uint64)
8       5      Name (5-char ASCII)
13      32     SHA256 Hash
────────────────────────────────────── 45 bytes per record
...
```

Size example: `11.8M × 45 bytes ≈ 535 MB`.

### B+ Tree Index File (.bpt)

4 KB fixed-size pages with a hierarchical layout.

```
Page Layout (4096 bytes):

┌─────────────────────────────────────────────────┐
│ Page 0: HEADER                                  │
├─────────────────────────────────────────────────┤
│ Magic: "BPTREE1\0"  (8 bytes)                   │
│ Page Size: 4096     (4 bytes)                   │
│ Root Page Number    (4 bytes)                   │
│ Record Count        (4 bytes)                   │
│ Leaf Capacity       (4 bytes)                   │
│ Internal Capacity   (4 bytes)                   │
│ First Leaf Page     (4 bytes)                   │
│ Reserved            (remaining bytes)           │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│ Pages 1+: LEAF & INTERNAL NODES                 │
├─────────────────────────────────────────────────┤
│ Node Type (1 byte): 1=LEAF, 2=INTERNAL          │
│ Entry Count        (4 bytes)                    │
│ Next Page (leaf only) (4 bytes)                 │
│ Entries:           (remainder of page)          │
│                                                 │
│ LEAF ENTRY: [hash (32 bytes) | id (8 bytes)]    │
│ INTERNAL ENTRY: [child_page (4 bytes) |         │
│                  separator_key (32 bytes)]      │
└─────────────────────────────────────────────────┘
```

Tree example:

```
                ┌──────────────┐
                │   Header P0  │
                │   Root: P9   │
                └───────┬──────┘
                        │
          ┌─────────────┼──────────────┐
          │             │              │
       ┌──▼───┐      ┌──▼───┐      ┌───▼──┐
       │ Int  │      │ Int  │      │ Int  │
       │ P6   │      │ P7   │      │ P8   │
       └──┬─┬─┘      └──┬─┬─┘      └──┬───┘
          │ │           │ └────────┐  │
        ┌─┘ └────┐      └─┐        │  └─────┐
        │        │        │        │        │
    ┌───▼──┐ ┌───▼──┐ ┌───▼──┐ ┌───▼──┐ ┌───▼──┐
    │ Leaf │ │ Leaf │ │ Leaf │ │ Leaf │ │ Leaf │
    │ P1   │ │ P2   │ │ P3   │ │ P4   │ │ P5   │
    └──────┘ └──┬───┘ └──────┘ └──────┘ └──────┘
               linked via next_page pointers ───→

Leaf pages contain: [hash₁, id₁] [hash₂, id₂] ... (sorted)
Internal pages contain: [child_page] [separator_key] [child_page] ...
```

Lookup path: root → internal nodes → leaf → binary search inside the leaf.

## Lookup Strategies

`DbEngine` supports two lookup modes:

- `linear`: scan the database file directly
- `bplus`: traverse `db/mixer_emulator.bpt`

Example:

```python
from emulator.storage.engine import DbEngine

db = DbEngine(lookup_strategy=DbEngine.STRATEGY_BPLUS)
result = db.query_by_hash(hash_bytes)
```

## Query Methods

| Method      | Time     | Space | Setup           |
| ----------- | -------- | ----- | --------------- |
| Linear scan | O(n)     | O(1)  | None            |
| B+ tree     | O(log n) | O(1)  | Build .bpt file |

## Example Benchmark (Single Run)

Sample output from one run on this project data.

| Step          | Result             | Time (ms) |
| ------------- | ------------------ | --------- |
| Read record   | id=7392683, qepxz  | 0.171     |
| Linear lookup | (7392683, 'qepxz') | 1832.717  |
| Sorted lookup | (7392683, 'qepxz') | 13.655    |
| B+ lookup     | (7392683, 'qepxz') | 1.156     |

Relative to linear lookup in this run:

| Strategy | Speedup vs linear |
| -------- | ----------------- |
| Linear   | 1.0x              |
| Sorted   | 134.2x            |
| B+ tree  | 1585.4x           |

This is an illustrative example, not a strict performance guarantee.

## Concurrency And Locking

`DbEngine` uses a sidecar lock file at `db/mixer_emulator_bin.db.lock`.

- Read operations acquire a shared lock
- Write operations acquire an exclusive lock
- Separate `DbEngine` instances can read concurrently
- Writes are serialized across processes and threads

This locking is implemented with platform-specific primitives and works on Windows and Unix-like systems.

## Update Operations

The B+ tree index supports writable updates with rollback safety.

### Via Database Class

```python
from emulator.storage.engine import DbEngine

db = DbEngine(lookup_strategy=DbEngine.STRATEGY_BPLUS)
# Update record's name and sync B-tree index
db.update_record_with_bplus_index(
    42,
    "zzzzz"  # Must be 5 lowercase ASCII letters
)
```

You can also use generic dispatch based on the configured lookup strategy.

This operation:

1. Reads the original record to get the old hash
2. Computes the new hash from (id, new_name)
3. Updates the database file and the active index
4. Rolls back database changes if index update fails

### Via B-tree Index Directly

```python
from emulator.storage.b_tree_index import BPlusTreeIndex

with BPlusTreeIndex(writable=True) as idx:
    success = idx.update(
        old_hash=old_hash_bytes,
        new_hash=new_hash_bytes,
        record_id=42
    )
```

### Implementation Details

The writable B+ tree path uses incremental page-local writes: follow one root-to-leaf path, update the target page, and propagate splits or separator changes only if needed.

The B+ tree write path no longer rebuilds the full tree for ordinary insert/update/delete operations.

### Insert and Delete Operations

| Index  | Insert/Delete behavior                                                |
| ------ | --------------------------------------------------------------------- |
| `.bpt` | Supports online writable insert/delete/update through the tree itself |

### Time Complexity

| Operation | `.idx`     | `.bpt`                                                        |
| --------- | ---------- | ------------------------------------------------------------- |
| Lookup    | `O(log n)` | `O(log n)`                                                    |
| Insert    | `O(n)`     | Typically `O(log n)`                                          |
| Update    | `O(n)`     | Typically `O(log n)`                                          |
| Delete    | `O(n)`     | Usually `O(log n)`; can be worse when unlinking an empty leaf |

**Limitations:**

| Area              | Current limitation                                |
| ----------------- | ------------------------------------------------- |
| Delete rebalance  | No full borrow/merge handling for underfull nodes |
| Empty-leaf unlink | May scan the leaf chain to find the previous leaf |
| Page reuse        | No free list/page reclamation yet                 |
