import math
import os
import struct
import mmap
from typing import Iterator, List, Tuple, Optional

PAGE_SIZE = 4096
KEY_SIZE = 32          # sha256
VAL_SIZE = 8           # uint64

# file header: root_page:uint64 | page_count:uint64
_FILE_HEADER_STRUCT = "<QQ"
_FILE_HEADER_SIZE = struct.calcsize(_FILE_HEADER_STRUCT)

# page header: is_leaf:uint8 | num_keys:uint16 | pad:uint8 | next_leaf:uint64
_PAGE_HEADER_STRUCT = "<B H B Q"
_PAGE_HEADER_SIZE = struct.calcsize(_PAGE_HEADER_STRUCT)

# leaf entry: key(32) + value(8)
_LEAF_ENTRY_STRUCT = f"<{KEY_SIZE}sQ"
_LEAF_ENTRY_SIZE = struct.calcsize(_LEAF_ENTRY_STRUCT)

# choose MAX_KEYS so header + MAX_KEYS * LEAF_ENTRY_SIZE <= PAGE_SIZE
MAX_KEYS = (PAGE_SIZE - _PAGE_HEADER_SIZE) // _LEAF_ENTRY_SIZE  #  (4096-12)//40 = 102
# MAX_KEYS is now 102, so we will never overflow a 4 KiB page

def _ensure_file(path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not os.path.exists(path):
        with open(path, "wb") as f:
            # root = 0 (none yet), page_count = 1 (header page only)
            f.write(struct.pack(_FILE_HEADER_STRUCT, 0, 1))
            f.write(b"\x00" * (PAGE_SIZE - _FILE_HEADER_SIZE))


class BTreeIndex:
    """
    On-disk B+ tree index with fixed-size keys (32 bytes) and values (uint64).

    Features:
      - query(key: bytes) -> Optional[int]
      - insert(key: bytes, value: int)
      - delete(key: bytes) -> bool
      - bulk_load(sorted_entries: Iterator[(key, id)])  # keys must be sorted

    Simplifications:
      - Single-writer, no WAL, best-effort fsync for page allocation.
      - delete does not rebalance/merge underfull nodes (fine for simulation).
    """

    def __init__(self, path: str):
        self.path = path
        _ensure_file(path)
        self._f = open(path, "r+b")
        self._mm = mmap.mmap(self._f.fileno(), 0)
        self._root, self._page_count = struct.unpack_from(_FILE_HEADER_STRUCT, self._mm, 0)

    # ---------------------- basic file helpers ----------------------

    def close(self) -> None:
        try:
            self._mm.flush()
            self._mm.close()
        finally:
            try:
                self._f.close()
            except Exception:
                pass

    def _sync_header(self) -> None:
        struct.pack_into(_FILE_HEADER_STRUCT, self._mm, 0, self._root, self._page_count)
        self._mm.flush()

    def _read_page(self, page_id: int) -> memoryview:
        off = page_id * PAGE_SIZE
        return memoryview(self._mm)[off: off + PAGE_SIZE]

    def _alloc_page(self) -> int:
        """
        Reserve a new page id within the already-mapped file.

        Assumes the file was pre-allocated large enough to hold all pages
        before constructing BTreeIndex (see build_b_tree_index).
        """
        new_page_id = self._page_count
        self._page_count += 1
        self._sync_header()
        return new_page_id

    # ---------------------- node read/write helpers ----------------------

    def _read_leaf(self, page_id: int) -> Tuple[List[bytes], List[int], int]:
        page = self._read_page(page_id)
        is_leaf, num_keys, _, next_leaf = struct.unpack_from(_PAGE_HEADER_STRUCT, page, 0)
        assert is_leaf == 1
        num_keys = int(num_keys)
        keys: List[bytes] = []
        vals: List[int] = []
        off = _PAGE_HEADER_SIZE
        for i in range(num_keys):
            k, v = struct.unpack_from(_LEAF_ENTRY_STRUCT, page, off + i * _LEAF_ENTRY_SIZE)
            keys.append(bytes(k))
            vals.append(int(v))
        return keys, vals, int(next_leaf)

    def _write_leaf(self, page_id: int, keys: List[bytes], vals: List[int], next_leaf: int) -> None:
        assert len(keys) == len(vals)
        page = bytearray(PAGE_SIZE)
        struct.pack_into(_PAGE_HEADER_STRUCT, page, 0, 1, len(keys), 0, next_leaf)
        off = _PAGE_HEADER_SIZE
        for i, (k, v) in enumerate(zip(keys, vals)):
            struct.pack_into(_LEAF_ENTRY_STRUCT, page, off + i * _LEAF_ENTRY_SIZE, k, v)
        self._mm[page_id * PAGE_SIZE: page_id * PAGE_SIZE + PAGE_SIZE] = bytes(page)

    def _read_internal(self, page_id: int) -> Tuple[List[bytes], List[int]]:
        page = self._read_page(page_id)
        is_leaf, num_keys, _, _ = struct.unpack_from(_PAGE_HEADER_STRUCT, page, 0)
        assert is_leaf == 0
        num_keys = int(num_keys)
        off_k = _PAGE_HEADER_SIZE
        keys: List[bytes] = []
        for i in range(num_keys):
            keys.append(bytes(page[off_k + i * KEY_SIZE: off_k + (i + 1) * KEY_SIZE]))
        child_area_off = off_k + (MAX_KEYS * KEY_SIZE)
        children: List[int] = []
        for i in range(num_keys + 1):
            (cid,) = struct.unpack_from("<Q", page, child_area_off + i * 8)
            children.append(int(cid))
        return keys, children

    def _write_internal(self, page_id: int, keys: List[bytes], children: List[int]) -> None:
        assert len(children) == len(keys) + 1
        page = bytearray(PAGE_SIZE)
        struct.pack_into(_PAGE_HEADER_STRUCT, page, 0, 0, len(keys), 0, 0)
        off_k = _PAGE_HEADER_SIZE
        for i, k in enumerate(keys):
            page[off_k + i * KEY_SIZE: off_k + (i + 1) * KEY_SIZE] = k
        child_area_off = off_k + (MAX_KEYS * KEY_SIZE)
        for i, cid in enumerate(children):
            struct.pack_into("<Q", page, child_area_off + i * 8, cid)
        self._mm[page_id * PAGE_SIZE: page_id * PAGE_SIZE + PAGE_SIZE] = bytes(page)

    # ---------------------- bulk load ----------------------
# ...existing code above...

    def bulk_load(self, sorted_entries: Iterator[Tuple[bytes, int]]) -> None:
        """
        Build tree from sorted (key, value) entries.

        NOTE: this overwrites existing tree content (conceptually).
        """
        # build leaf level
        leaf_pages: List[int] = []
        cur_keys: List[bytes] = []
        cur_vals: List[int] = []

        def flush_leaf(next_leaf: int) -> None:
            nonlocal cur_keys, cur_vals
            if not cur_keys:
                return
            pid = self._alloc_page()
            leaf_pages.append(pid)
            self._write_leaf(pid, cur_keys, cur_vals, next_leaf)
            cur_keys = []
            cur_vals = []

        for k, v in sorted_entries:
            if len(k) != KEY_SIZE:
                raise ValueError("key must be 32 bytes")
            cur_keys.append(k)
            cur_vals.append(v)
            if len(cur_keys) >= MAX_KEYS:
                flush_leaf(0)
        flush_leaf(0)

        # patch next_leaf pointers
        for i in range(len(leaf_pages) - 1):
            pid = leaf_pages[i]
            keys, vals, _ = self._read_leaf(pid)
            self._write_leaf(pid, keys, vals, leaf_pages[i + 1])

        # build internal levels upwards
        child_level = leaf_pages
        while len(child_level) > 1:
            parents: List[int] = []
            i = 0
            while i < len(child_level):
                group = child_level[i: i + (MAX_KEYS + 1)]
                # separator keys: first key from each child except first
                parent_keys: List[bytes] = []
                for child_pid in group[1:]:
                    ck = self._read_node_first_key(child_pid)
                    parent_keys.append(ck)
                pid = self._alloc_page()
                self._write_internal(pid, parent_keys, group)
                parents.append(pid)
                i += (MAX_KEYS + 1)
            child_level = parents

        # pick root
        if child_level:
            self._root = child_level[0]
        elif leaf_pages:
            self._root = leaf_pages[0]
        else:
            self._root = 0
        self._sync_header()
        self._mm.flush()

    def _read_node_first_key(self, page_id: int) -> bytes:
        """
        Return the first key of a node (leaf or internal).
        Used during bulk-load when building parents.
        """
        page = self._read_page(page_id)
        is_leaf, num_keys, _, _ = struct.unpack_from(_PAGE_HEADER_STRUCT, page, 0)
        num_keys = int(num_keys)
        if num_keys == 0:
            raise RuntimeError("node without keys")

        if is_leaf == 1:
            # first leaf key
            off = _PAGE_HEADER_SIZE
            k, _ = struct.unpack_from(_LEAF_ENTRY_STRUCT, page, off)
            return bytes(k)
        else:
            # first internal key
            off_k = _PAGE_HEADER_SIZE
            return bytes(page[off_k: off_k + KEY_SIZE])

    # ---------------------- lookup ----------------------

    def query(self, key: bytes) -> Optional[int]:
        """Exact match lookup for 32-byte key. Returns value (id) or None."""
        if len(key) != KEY_SIZE:
            raise ValueError("key must be 32 bytes")
        if self._root == 0:
            return None

        page_id = self._root
        while True:
            page = self._read_page(page_id)
            is_leaf, num_keys, _, next_leaf = struct.unpack_from(_PAGE_HEADER_STRUCT, page, 0)
            num_keys = int(num_keys)
            if is_leaf:
                # binary search in leaf
                lo, hi = 0, num_keys - 1
                base_off = _PAGE_HEADER_SIZE
                while lo <= hi:
                    mid = (lo + hi) // 2
                    off = base_off + mid * _LEAF_ENTRY_SIZE
                    k = bytes(page[off: off + KEY_SIZE])
                    if k == key:
                        _, v = struct.unpack_from(_LEAF_ENTRY_STRUCT, page, off)
                        return int(v)
                    elif k < key:
                        lo = mid + 1
                    else:
                        hi = mid - 1
                return None
            else:
                # descend internal
                off_k = _PAGE_HEADER_SIZE
                lo, hi = 0, num_keys - 1
                idx = None
                while lo <= hi:
                    mid = (lo + hi) // 2
                    k = bytes(page[off_k + mid * KEY_SIZE: off_k + (mid + 1) * KEY_SIZE])
                    if key < k:
                        idx = mid
                        hi = mid - 1
                    else:
                        lo = mid + 1
                child_index = num_keys if idx is None else idx
                child_area_off = off_k + (MAX_KEYS * KEY_SIZE)
                (child_pid,) = struct.unpack_from("<Q", page, child_area_off + child_index * 8)
                page_id = int(child_pid)

    # ---------------------- insert ----------------------

    def insert(self, key: bytes, value: int) -> None:
        if len(key) != KEY_SIZE:
            raise ValueError("key must be 32 bytes")

        # special case: empty tree -> single leaf
        if self._root == 0:
            pid = self._alloc_page()
            self._write_leaf(pid, [key], [value], 0)
            self._root = pid
            self._sync_header()
            return

        # descend, keeping path of internal node ids
        path: List[int] = []
        page_id = self._root
        while True:
            page = self._read_page(page_id)
            is_leaf, num_keys, _, _ = struct.unpack_from(_PAGE_HEADER_STRUCT, page, 0)
            if is_leaf:
                break
            path.append(page_id)
            off_k = _PAGE_HEADER_SIZE
            lo, hi = 0, int(num_keys) - 1
            idx = None
            while lo <= hi:
                mid = (lo + hi) // 2
                k = bytes(page[off_k + mid * KEY_SIZE: off_k + (mid + 1) * KEY_SIZE])
                if key < k:
                    idx = mid
                    hi = mid - 1
                else:
                    lo = mid + 1
            child_index = int(num_keys) if idx is None else idx
            child_area_off = off_k + (MAX_KEYS * KEY_SIZE)
            (child_pid,) = struct.unpack_from("<Q", page, child_area_off + child_index * 8)
            page_id = int(child_pid)

        # modify leaf
        keys, vals, next_leaf = self._read_leaf(page_id)
        import bisect
        pos = bisect.bisect_left(keys, key)
        if pos < len(keys) and keys[pos] == key:
            # replace existing
            vals[pos] = value
            self._write_leaf(page_id, keys, vals, next_leaf)
            return

        keys.insert(pos, key)
        vals.insert(pos, value)

        split_key: Optional[bytes] = None
        new_child: Optional[int] = None

        if len(keys) <= MAX_KEYS:
            self._write_leaf(page_id, keys, vals, next_leaf)
        else:
            # split leaf
            mid = len(keys) // 2
            left_keys = keys[:mid]
            left_vals = vals[:mid]
            right_keys = keys[mid:]
            right_vals = vals[mid:]
            new_pid = self._alloc_page()
            # original becomes left; new becomes right
            self._write_leaf(page_id, left_keys, left_vals, new_pid)
            self._write_leaf(new_pid, right_keys, right_vals, next_leaf)
            split_key = right_keys[0]  # promoted separator
            new_child = new_pid

        # propagate splits up the path
        while split_key is not None and new_child is not None:
            if not path:
                # need new root
                root_pid = self._alloc_page()
                self._write_internal(root_pid, [split_key], [self._root, new_child])
                self._root = root_pid
                self._sync_header()
                break

            parent_id = path.pop()
            p_keys, p_children = self._read_internal(parent_id)
            import bisect
            pos = bisect.bisect_left(p_keys, split_key)
            p_keys.insert(pos, split_key)
            p_children.insert(pos + 1, new_child)

            if len(p_keys) <= MAX_KEYS:
                self._write_internal(parent_id, p_keys, p_children)
                split_key = None
                new_child = None
            else:
                # split internal
                mid = len(p_keys) // 2
                promote = p_keys[mid]
                left_keys = p_keys[:mid]
                right_keys = p_keys[mid + 1 :]
                left_children = p_children[: mid + 1]
                right_children = p_children[mid + 1 :]
                new_pid = self._alloc_page()
                self._write_internal(parent_id, left_keys, left_children)
                self._write_internal(new_pid, right_keys, right_children)
                split_key = promote
                new_child = new_pid

    # ---------------------- delete (no rebalancing) ----------------------

    def delete(self, key: bytes) -> bool:
        """Delete key if present. Returns True if removed, False if not found."""
        if len(key) != KEY_SIZE:
            raise ValueError("key must be 32 bytes")
        if self._root == 0:
            return False

        page_id = self._root
        while True:
            page = self._read_page(page_id)
            is_leaf, num_keys, _, _ = struct.unpack_from(_PAGE_HEADER_STRUCT, page, 0)
            num_keys = int(num_keys)
            if is_leaf:
                break
            off_k = _PAGE_HEADER_SIZE
            lo, hi = 0, num_keys - 1
            idx = None
            while lo <= hi:
                mid = (lo + hi) // 2
                k = bytes(page[off_k + mid * KEY_SIZE: off_k + (mid + 1) * KEY_SIZE])
                if key < k:
                    idx = mid
                    hi = mid - 1
                else:
                    lo = mid + 1
            child_index = num_keys if idx is None else idx
            child_area_off = off_k + (MAX_KEYS * KEY_SIZE)
            (child_pid,) = struct.unpack_from("<Q", page, child_area_off + child_index * 8)
            page_id = int(child_pid)

        keys, vals, next_leaf = self._read_leaf(page_id)
        try:
            pos = keys.index(key)
        except ValueError:
            return False
        del keys[pos]
        del vals[pos]
        self._write_leaf(page_id, keys, vals, next_leaf)
        return True
    

def build_b_tree_index():
    from .constants import DEFAULT_DB_PATH, DB_RECORD_SIZE, DB_HASH_OFFSET, DEFAULT_INDEX_PATH

    total = os.path.getsize(DEFAULT_DB_PATH) // DB_RECORD_SIZE

    # --- preallocate index file so mmap is large enough ---
    # leaf pages: ceil(total / MAX_KEYS)
    leaf_pages = max(1, math.ceil(total / MAX_KEYS))
    # internal pages upper bound (very rough, but safe): same as leaf_pages
    internal_pages = max(1, leaf_pages)
    # 1 header page + leaf + internal
    total_pages = 1 + leaf_pages + internal_pages

    os.makedirs(os.path.dirname(DEFAULT_INDEX_PATH) or ".", exist_ok=True)
    with open(DEFAULT_INDEX_PATH, "wb") as f:
        f.truncate(total_pages * PAGE_SIZE)

    # now build sorted entries and bulk-load
    def iter_sorted():
        with open(DEFAULT_DB_PATH, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            items = []
            for i in range(total):
                off = i * DB_RECORD_SIZE
                h = mm[off + DB_HASH_OFFSET : off + DB_HASH_OFFSET + 32]
                items.append((h, i))
            items.sort(key=lambda x: x[0])
            for kv in items:
                yield kv
            mm.close()

    idx = BTreeIndex(DEFAULT_INDEX_PATH)
    idx.bulk_load(iter_sorted())
    idx.close()

    print("B+Tree index built at", DEFAULT_INDEX_PATH)