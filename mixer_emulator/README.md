# Mixer Emulator

## Overview

This repository is a Python file-backed database emulator. It includes fixed-size binary records, hash lookup, a sorted flat index, a B+ tree index, and a simple microservice wrapper.

## Project Structure

```
mixer_emulator/
├── src/
│   ├── emulator/
│   │   ├── __init__.py
│   │   ├── client.py
│   │   ├── main.py
│   │   ├── service.py
│   │   ├── storage/
│   │   │   ├── __init__.py
│   │   │   ├── b_tree_index.py
│   │   │   ├── constants.py
│   │   │   ├── database.py
│   │   │   └── sorted_index.py
│   │   └── utils.py
│   └── tests/
│       ├── __init__.py
│       ├── test_b_tree_index.py
│       ├── test_sorted_index.py
│       └── test_utils.py
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Installation

Install dependencies:

```bash
pip install -r requirements.txt
```

Install in editable mode:

```bash
pip install -e .
```

## Usage

Build the database:

```bash
python -m emulator.storage.database
```

Build the sorted index:

```bash
python -m emulator.storage.sorted_index
```

Build the B+ tree index:

```bash
python -m emulator.storage.b_tree_index
```

Run the demo:

```bash
python -m emulator.main
```

## Running Tests

Run tests:

```bash
pytest src/tests
```

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
                    │    Header    │ (Page 0)
                    │   Root: P3   │
                    └───────┬──────┘
                            │
              ┌─────────────┼──────────────┐
              │             │              │
           ┌──▼───┐      ┌──▼───┐      ┌───▼──┐
           │ Int  │      │ Int  │      │ Int  │
           │ P3   │      │ P4   │      │ P5   │
           └──┬─┬─┘      └──┬─┬─┘      └──┬───┘
              │ │           │ │           │
        ┌─────┘ │      ┌────┘ └─┐       ┌─┘
        │       │      │        │       │
    ┌───▼──┐ ┌──▼──┐ ┌──▼──┐ ┌──▼──┐ ┌──▼──┐
    │ Leaf │ │ Leaf│ │ Leaf│ │ Leaf│ │ Leaf│
    │ P1   │ │ P2  │ │ P6  │ │ P7  │ │ P8  │
    └──────┘ └──┬──┘ └─────┘ └─────┘ └─────┘
               linked via next_page pointers ───→

Leaf pages contain: [hash₁, id₁] [hash₂, id₂] ... (sorted)
Internal pages contain: [child_page] [separator_key] [child_page] ...
```

Lookup path: root → internal nodes → leaf → binary search inside the leaf.

## Query Methods

| Method                | Time     | Space | Setup           |
| --------------------- | -------- | ----- | --------------- |
| Linear scan           | O(n)     | O(1)  | None            |
| Binary search on .idx | O(log n) | O(1)  | Build .idx file |
| B+ tree               | O(log n) | O(1)  | Build .bpt file |

## Update Operations

The B+ tree supports writable updates. The sorted index does not; after data changes, `.idx` must be rebuilt.

### Via Database Class

```python
from emulator.storage.database import FileDB

db = FileDB(lookup_strategy=FileDB.STRATEGY_BPLUS)
# Update record's name and sync B-tree index
db.update_record_with_bplus_index(
    record_id=42,
    new_name="newname"  # Must be 5 lowercase ASCII letters
)
```

This operation:

1. Reads the original record to get the old hash
2. Computes the new hash from (id, new_name)
3. Updates the database file and B+ tree index atomically
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

The B+ tree uses incremental writes: follow one root-to-leaf path, update the target page, and propagate splits or separator changes only if needed. That is why `.bpt` can be updated online while `.idx` still needs a rebuild.

### Insert and Delete Operations

| Index  | Insert/Delete behavior                                                                               |
| ------ | ---------------------------------------------------------------------------------------------------- |
| `.idx` | Global sort must be preserved, so the file is rebuilt with `python -m emulator.storage.sorted_index` |
| `.bpt` | Supports online writable insert/delete/update through the tree itself                                |

### Time Complexity

| Operation | `.idx`                            | `.bpt`                                                        |
| --------- | --------------------------------- | ------------------------------------------------------------- |
| Lookup    | `O(log n)`                        | `O(log n)`                                                    |
| Insert    | Rebuild, effectively `O(n log n)` | Typically `O(log n)`                                          |
| Update    | Rebuild, effectively `O(n log n)` | Typically `O(log n)`                                          |
| Delete    | Rebuild, effectively `O(n log n)` | Usually `O(log n)`; can be worse when unlinking an empty leaf |

**Limitations:**

| Area              | Current limitation                                |
| ----------------- | ------------------------------------------------- |
| Delete rebalance  | No full borrow/merge handling for underfull nodes |
| Empty-leaf unlink | May scan the leaf chain to find the previous leaf |
| Page reuse        | No free list/page reclamation yet                 |
