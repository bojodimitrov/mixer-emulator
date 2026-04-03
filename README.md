# Microservices Emulator

## Overview

This repository is a Python data-system emulator built on a local file database, hash indexes, and TCP service layers.

It includes:

- A mmap-backed file database with cross-process read/write locking
- Two lookup strategies: linear scan and B+ tree index
- In-process orchestration (`DbOrchestrator`) plus socket server/client layers (`DbServer`, `DbClient`)
- A microservice layer (`MicroserviceServer`, `MicroserviceClient`) and frontend runners for corruption/repair flows
- Writable B+ tree update path with rollback on index update failure

## Problems that forced evolution

Database Linear search -> sorted index
Update and maintain index -> B+ tree index
Scalability of database -> thread executor
Cold starts -> thread pool ready to receive requests
Multiple threads updating DB files -> Row-level locking and page level locking
Connection churn (Only one usage of each socket address... is normally permitted) ->

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
│   │   ├── frontend/
│   │   │   ├── ...
│   │   ├── microservice/
│   │   │   ├── ...
│   │   ├── orchestrator/
│   │   │   ├── ...
│   │   ├── demonstrations/
│   │   ├── storage/
│   │   │   ├── ...
│   │   ├── transport_layer/
│   │   └── utils.py
│   └── tests/
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Storage Layout

The project uses a src layout. Install it in editable mode or set `PYTHONPATH=src` before running modules directly.

Default generated files:

- `db/mixer_emulator_bin.db`: fixed-size record database
- `db/mixer_emulator.bpt`: B+ tree index
- `db/tmp_btree/`: temporary chunk files used while building `.bpt`

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

Build the database:

```bash
python -m emulator.storage.database
```

Build a smaller database slice for quick local testing:

```bash
python -m emulator.storage.database --start 0 --end 100000
```

Build the B+ tree index:

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

Choose DB lookup strategy:

```bash
python -m emulator.main --lookup-strategy bplus
python -m emulator.main --lookup-strategy linear
```

Main orchestration flags:

- `--corrupters <int>`: number of corrupter client workers (default: `2`)
- `--repairers <int>`: number of repairer client workers (default: `2`)
- `--client-pause-ms <float>`: pause between client operations (default: `20.0`)
- `--lookup-strategy <linear|bplus>`: DB lookup backend (default: `bplus`)
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

## Socket-based Decoupling (DB -> Service -> Frontend)

In addition to the in-process queue-based components, the repo includes a small JSON-over-TCP layer that lets you run the database, microservice, and frontend as separate processes.

Key modules:

- `emulator/transport_layer/transport.py`: length-prefixed JSON framing (`send_message` / `recv_message`)
- `emulator/storage/server.py`: `DbServer` (TCP wrapper around `DbOrchestrator`)
- `emulator/storage/client.py`: `DbClient`
- `emulator/microservice/server.py`: `MicroserviceServer`
- `emulator/microservice/client.py`: `MicroserviceClient`

### Architecture diagram

This shows the request flow inside `DbServer`, and specifically what `self._thread` does.

```text
Caller thread
  |
  |  start()
  v
+---------------------------+
| DbServer                  |
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
       | loop:
       |   accept()  (with timeout)
       v
   +-------------------+
   | new TCP conn      |  (conn socket)
   +-------------------+
       |
       | submit to thread pool
       v
+----------------------------------+
| _executor: ThreadPoolExecutor    |  (N worker threads)
+----------------------------------+
       |
       | runs in a worker:
       v
   +--------------------------+
   | _handle_conn(conn)       |
   |  - sets TCP_NODELAY      |
   |  - sets conn timeout     |
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

The suite currently includes server, client, transport, and storage tests.

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
