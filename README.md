# Microservices Emulator

## Overview

This repository is a Python data-system emulator built on a local file database, hash indexes, and TCP service layers.

It includes:

- A mmap-backed file database with cross-process read/write locking
- Three lookup strategies: linear scan, sorted flat index, and B+ tree index
- In-process orchestration (`DbOrchestrator`) plus socket server/client layers (`DbServer`, `DbClient`)
- A microservice layer (`MicroserviceServer`, `MicroserviceClient`) and frontend runners for corruption/repair flows
- Writable sorted-index and B+ tree update paths with rollback on index update failure

## Project Structure

```
.
├── db/
│   ├── mixer_emulator_bin.db
│   ├── mixer_emulator.bpt
│   ├── mixer_emulator.idx
│   ├── tmp/
│   └── tmp_btree/
├── src/
│   ├── emulator/
│   │   ├── __init__.py
│   │   ├── main.py
│   │   ├── frontend/
│   │   │   └── clients.py
│   │   ├── microservice/
│   │   │   ├── client.py
│   │   │   ├── framework.py
│   │   │   └── server.py
│   │   ├── demonstrations/
│   │   │   ├── __init__.py
│   │   │   └── database_demo.py
│   │   ├── storage/
│   │   │   ├── __init__.py
│   │   │   ├── b_tree_index.py
│   │   │   ├── client.py
│   │   │   ├── constants.py
│   │   │   ├── database.py
│   │   │   ├── engine.py
│   │   │   ├── orchestrator.py
│   │   │   ├── server.py
│   │   │   └── sorted_index.py
│   │   ├── transport_layer/
│   │   │   └── transport.py
│   │   └── utils.py
│   └── tests/
│       ├── __init__.py
│       ├── test_b_tree_index.py
│       ├── test_file_db.py
│       ├── test_frontend_clients.py
│       ├── test_server_components.py
│       ├── test_server_connection.py
│       ├── test_sorted_index.py
│       └── test_utils.py
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Storage Layout

The project uses a src layout. Install it in editable mode or set `PYTHONPATH=src` before running modules directly.

Default generated files:

- `db/mixer_emulator_bin.db`: fixed-size record database
- `db/mixer_emulator.idx`: sorted `(hash, id)` flat index
- `db/mixer_emulator.bpt`: B+ tree index
- `db/tmp/`: temporary chunk files used while building `.idx`
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

Build the sorted index:

```bash
python -m emulator.storage.sorted_index
```

Build the B+ tree index:

```bash
python -m emulator.storage.b_tree_index
```

Run the demo application:

```bash
python -m emulator.main
```

The demo:

- Creates three `DbOrchestrator` instances, one per lookup strategy
- Reads a random record directly from the database
- Compares linear, sorted-index, and B+ tree lookup latency
- Executes a writable B+ tree update and verifies the old hash disappears

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
pytest src/tests
```

Current test coverage includes:

- Core database read and linear-hash lookup behavior
- Concurrent reads across multiple `DbEngine` connections
- Sorted index build, lookup, insert, delete, and update dispatch
- B+ tree build, lookup, insert, delete, update, and small-capacity regression cases

The suite currently includes server, client, transport, and storage tests.

## Benchmark Snapshot (Corrupt Then Repair Demo)

Latest run shape: `Repair (clean) -> Corrupt -> Repair (corrupted)`, 10 IDs per phase.

| Strategy               | Avg repair clean (ms) | Avg corrupt (ms) | Avg repair corrupted (ms) |
| ---------------------- | --------------------: | ---------------: | ------------------------: |
| No index (linear scan) |                800.82 |             7.33 |                   2719.60 |
| Sorted index           |                  5.97 |           929.77 |                   1104.98 |
| B+ tree                |                 18.91 |            38.38 |                     23.38 |

Repair clean will execute only reads
Corrupt will execute only writes
Repair corrupted will execute both reads and writes

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

### Sorted Index File (.idx)

Sorted `(hash, id)` pairs for binary search.

```
Offset  Size   Field
──────────────────────────
0       32     Hash (SHA256)
32      8      Record ID (uint64)
────────────────────────── 40 bytes per entry
...
```

Sorted by hash in ascending byte order. Built with external sort plus merge.

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

`DbEngine` supports three lookup modes:

- `linear`: scan the database file directly
- `sorted`: binary search against `db/mixer_emulator.idx`
- `bplus`: traverse `db/mixer_emulator.bpt`

Example:

```python
from emulator.storage.engine import DbEngine

db = DbEngine(lookup_strategy=DbEngine.STRATEGY_BPLUS)
result = db.query_by_hash(hash_bytes)
```

## Query Methods

| Method                | Time     | Space | Setup           |
| --------------------- | -------- | ----- | --------------- |
| Linear scan           | O(n)     | O(1)  | None            |
| Binary search on .idx | O(log n) | O(1)  | Build .idx file |
| B+ tree               | O(log n) | O(1)  | Build .bpt file |

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

Both on-disk index types now have writable update support, but with different tradeoffs.

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

You can also use generic dispatch based on the configured lookup strategy:

```python
from emulator.storage.engine import DbEngine

db = DbEngine(lookup_strategy=DbEngine.STRATEGY_SORTED)
db.update_record(42, "zzzzz")
```

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

### Via Sorted Index Directly

```python
from emulator.storage.sorted_index import HashIndex

with HashIndex(writable=True) as idx:
    idx.delete(old_hash_bytes)
    idx.insert(new_hash_bytes, 42)
```

### Implementation Details

The two writable index paths behave differently:

- `.idx` remains a sorted flat file, so insert/delete/update shift file contents and are `O(n)` in the size of the index
- `.bpt` uses incremental page-local writes: follow one root-to-leaf path, update the target page, and propagate splits or separator changes only if needed

The B+ tree write path no longer rebuilds the full tree for ordinary insert/update/delete operations.

### Insert and Delete Operations

| Index  | Insert/Delete behavior                                                     |
| ------ | -------------------------------------------------------------------------- |
| `.idx` | Writable, but entries are shifted in-place so large files remain expensive |
| `.bpt` | Supports online writable insert/delete/update through the tree itself      |

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
