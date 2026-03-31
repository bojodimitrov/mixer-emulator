import argparse
import heapq
import math
import mmap
import os
import struct
import tempfile
import time
from typing import Iterator, List, Optional, Tuple

from .constants import (
    DB_RECORD_SIZE,
    DB_HASH_OFFSET,
    INDEX_STRUCT,
    INDEX_RECORD_SIZE,
    DEFAULT_BPLUS_INDEX_PATH,
    DEFAULT_DB_PATH,
)


PAGE_SIZE = 4096
HEADER_MAGIC = b"BPTREE1\0"
HEADER_STRUCT = struct.Struct("<8sIIIIII")
HEADER_PAGE = 0

NODE_TYPE_LEAF = 1
NODE_TYPE_INTERNAL = 2

LEAF_HEADER_STRUCT = struct.Struct("<B3xII")
INTERNAL_HEADER_STRUCT = struct.Struct("<B3xI")

LEAF_ENTRY_SIZE = 40
LEAF_CAPACITY = (PAGE_SIZE - LEAF_HEADER_STRUCT.size) // LEAF_ENTRY_SIZE
INTERNAL_CAPACITY = (PAGE_SIZE - INTERNAL_HEADER_STRUCT.size - 4) // 36


def _page_offset(page_number: int) -> int:
    return page_number * PAGE_SIZE


class BPlusTreeBuilder:
    def __init__(
        self,
        db_path: Optional[str] = None,
        out_path: Optional[str] = None,
        chunk_size: int = 200_000,
    ):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.out_path = out_path or DEFAULT_BPLUS_INDEX_PATH
        self.chunk_size = int(chunk_size)
        self.tmp_dir = os.path.join(os.path.dirname(self.out_path), "tmp_btree")
        os.makedirs(self.tmp_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.out_path) or ".", exist_ok=True)

    def build(self) -> None:
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"database file not found: {self.db_path}")

        total_records = os.path.getsize(self.db_path) // DB_RECORD_SIZE
        if total_records == 0:
            raise ValueError(
                "database is empty; build it first with `python -m emulator.storage.database`"
            )

        # Extract and sort (hash, id) pairs from database using external sort
        sorted_pairs_path = os.path.join(self.tmp_dir, "sorted_pairs.tmp")
        self._extract_and_sort_from_db(total_records, sorted_pairs_path)

        # Build B+ tree from sorted pairs
        with open(sorted_pairs_path, "rb") as src, open(self.out_path, "w+b") as dst:
            dst.truncate(PAGE_SIZE)
            level_pages = self._write_leaf_level(src, dst, total_records)
            while len(level_pages) > 1:
                level_pages = self._write_internal_level(dst, level_pages)

            root_page = level_pages[0]
            header = HEADER_STRUCT.pack(
                HEADER_MAGIC,
                PAGE_SIZE,
                root_page,
                total_records,
                LEAF_CAPACITY,
                INTERNAL_CAPACITY,
                HEADER_PAGE + 1,
            )
            dst.seek(0)
            dst.write(header)
            dst.flush()

        # Clean up temporary file
        try:
            os.remove(sorted_pairs_path)
        except OSError:
            pass

    def _extract_and_sort_from_db(self, total_records: int, out_path: str) -> None:
        """Extract (hash, id) pairs from database and sort using external sort."""
        tmp_files: List[str] = []
        chunk_count = (total_records + self.chunk_size - 1) // self.chunk_size

        with open(self.db_path, "rb") as dbf:
            db_mm = mmap.mmap(dbf.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                for chunk_number, start in enumerate(
                    range(0, total_records, self.chunk_size), 1
                ):
                    end = min(total_records, start + self.chunk_size)
                    print(
                        f"Extracting chunk {chunk_number}/{chunk_count}: records {start}..{end - 1}",
                        flush=True,
                    )
                    entries: List[Tuple[bytes, int]] = []
                    for id_ in range(start, end):
                        off = id_ * DB_RECORD_SIZE
                        hb = db_mm[off + DB_HASH_OFFSET : off + DB_HASH_OFFSET + 32]
                        entries.append((hb, id_))
                    entries.sort(key=lambda x: x[0])

                    fd, tmp_path = tempfile.mkstemp(
                        prefix="bpt_chunk_", dir=self.tmp_dir
                    )
                    os.close(fd)
                    with open(tmp_path, "wb") as tf:
                        for hb, id_ in entries:
                            tf.write(struct.pack(INDEX_STRUCT, hb, id_))
                    tmp_files.append(tmp_path)
                    del entries
            finally:
                db_mm.close()

        print(f"Merging {len(tmp_files)} chunk files", flush=True)
        self._merge_sorted_chunks(tmp_files, out_path)

        for p in tmp_files:
            try:
                os.remove(p)
            except OSError:
                pass

    def _iter_chunk(self, path: str) -> Iterator[Tuple[bytes, int]]:
        """Iterator over (hash, id) pairs from a chunk file."""
        with open(path, "rb") as f:
            while True:
                data = f.read(INDEX_RECORD_SIZE)
                if not data:
                    break
                hb, id_ = struct.unpack(INDEX_STRUCT, data)
                yield (hb, id_)

    def _merge_sorted_chunks(self, chunk_paths: List[str], out_path: str) -> None:
        """Merge sorted chunk files into a single sorted output file."""
        iterators = [self._iter_chunk(p) for p in chunk_paths]
        with open(out_path, "wb") as out:
            for hb, id_ in heapq.merge(*iterators):
                out.write(struct.pack(INDEX_STRUCT, hb, id_))

    def _write_leaf_level(self, src, dst, total_records: int) -> List[int]:
        leaf_pages: List[int] = []
        leaf_count = math.ceil(total_records / LEAF_CAPACITY)

        for leaf_number in range(leaf_count):
            page_number = HEADER_PAGE + 1 + leaf_number
            key_count = min(LEAF_CAPACITY, total_records - leaf_number * LEAF_CAPACITY)
            next_page = page_number + 1 if leaf_number < leaf_count - 1 else 0
            page = bytearray(PAGE_SIZE)
            LEAF_HEADER_STRUCT.pack_into(page, 0, NODE_TYPE_LEAF, key_count, next_page)

            for entry_index in range(key_count):
                record = src.read(INDEX_RECORD_SIZE)
                if len(record) != INDEX_RECORD_SIZE:
                    raise ValueError(
                        "unexpected end of sorted index while building leaf level"
                    )
                offset = LEAF_HEADER_STRUCT.size + entry_index * LEAF_ENTRY_SIZE
                page[offset : offset + INDEX_RECORD_SIZE] = record

            dst.seek(_page_offset(page_number))
            dst.write(page)
            leaf_pages.append(page_number)

        return leaf_pages

    def _write_internal_level(self, dst, child_pages: List[int]) -> List[int]:
        parent_pages: List[int] = []
        start_page_number = dst.seek(0, os.SEEK_END) // PAGE_SIZE

        chunk_size = INTERNAL_CAPACITY + 1
        for group_index, start in enumerate(range(0, len(child_pages), chunk_size)):
            children = child_pages[start : start + chunk_size]
            key_count = len(children) - 1
            page_number = start_page_number + group_index
            page = bytearray(PAGE_SIZE)
            INTERNAL_HEADER_STRUCT.pack_into(page, 0, NODE_TYPE_INTERNAL, key_count)

            child_base = INTERNAL_HEADER_STRUCT.size
            key_base = child_base + (key_count + 1) * 4

            for child_index, child_page in enumerate(children):
                struct.pack_into("<I", page, child_base + child_index * 4, child_page)

            for key_index in range(key_count):
                first_key = self._read_first_key(dst, children[key_index + 1])
                key_offset = key_base + key_index * 32
                page[key_offset : key_offset + 32] = first_key

            dst.seek(_page_offset(page_number))
            dst.write(page)
            parent_pages.append(page_number)

        return parent_pages

    def _read_first_key(self, dst, page_number: int) -> bytes:
        dst.seek(_page_offset(page_number))
        node_type = struct.unpack("<B", dst.read(1))[0]
        if node_type == NODE_TYPE_LEAF:
            dst.seek(_page_offset(page_number) + LEAF_HEADER_STRUCT.size)
            key = dst.read(32)
            if len(key) != 32:
                raise ValueError("leaf page does not contain a complete key")
            return key

        if node_type == NODE_TYPE_INTERNAL:
            dst.seek(_page_offset(page_number))
            header = dst.read(INTERNAL_HEADER_STRUCT.size)
            _, key_count = INTERNAL_HEADER_STRUCT.unpack(header)
            child_page = struct.unpack("<I", dst.read(4))[0]
            if key_count < 0:
                raise ValueError("invalid internal node header")
            return self._read_first_key(dst, child_page)

        raise ValueError(f"unknown node type in B+ tree: {node_type}")


class BPlusTreeIndex:
    def __init__(self, path: Optional[str] = None, writable: bool = False):
        self.path = path or DEFAULT_BPLUS_INDEX_PATH
        mode = "r+b" if writable else "rb"
        self._file = open(self.path, mode)
        self._writable = writable
        header_data = self._file.read(HEADER_STRUCT.size)

        if len(header_data) != HEADER_STRUCT.size:
            self.close()
            raise ValueError("invalid B+ tree header")

        (
            magic,
            self.page_size,
            self.root_page,
            self.record_count,
            self.leaf_capacity,
            self.internal_capacity,
            self.first_node_page,
        ) = HEADER_STRUCT.unpack(header_data)
        if magic != HEADER_MAGIC:
            self.close()
            raise ValueError(f"invalid B+ tree magic in {self.path}")

        # Track modified pages for header updates
        self._record_count = self.record_count
        self._dirty = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def query_by_hash(self, hash_bytes: bytes) -> Optional[int]:
        page_number = self.root_page

        while True:
            node_type = self._read_node_type(page_number)
            if node_type == NODE_TYPE_LEAF:
                return self._search_leaf(page_number, hash_bytes)
            if node_type == NODE_TYPE_INTERNAL:
                page_number = self._search_internal(page_number, hash_bytes)
                continue
            raise ValueError(f"unknown node type in B+ tree: {node_type}")

    def iter_leaves(self) -> Iterator[Tuple[bytes, int]]:
        page_number = self._leftmost_leaf_page()
        while page_number:
            page = self._read_page(page_number)
            _, key_count, next_page = LEAF_HEADER_STRUCT.unpack_from(page, 0)

            for index in range(key_count):
                offset = LEAF_HEADER_STRUCT.size + index * LEAF_ENTRY_SIZE
                key, record_id = struct.unpack_from("<32sQ", page, offset)
                yield key, record_id

            page_number = next_page

    def _leftmost_leaf_page(self) -> int:
        page_number = self.root_page
        while self._read_node_type(page_number) == NODE_TYPE_INTERNAL:
            page = self._read_page(page_number)
            page_number = struct.unpack_from("<I", page, INTERNAL_HEADER_STRUCT.size)[0]
        return page_number

    def _read_node_type(self, page_number: int) -> int:
        self._file.seek(_page_offset(page_number))
        return struct.unpack("<B", self._file.read(1))[0]

    def _read_page(self, page_number: int) -> bytes:
        self._file.seek(_page_offset(page_number))
        page = self._file.read(PAGE_SIZE)
        if len(page) != PAGE_SIZE:
            raise ValueError(f"failed to read page {page_number}")
        return page

    def _search_internal(self, page_number: int, hash_bytes: bytes) -> int:
        page = self._read_page(page_number)
        _, key_count = INTERNAL_HEADER_STRUCT.unpack_from(page, 0)
        child_base = INTERNAL_HEADER_STRUCT.size
        key_base = child_base + (key_count + 1) * 4

        for index in range(key_count):
            key_offset = key_base + index * 32
            separator = page[key_offset : key_offset + 32]
            if hash_bytes < separator:
                return struct.unpack_from("<I", page, child_base + index * 4)[0]

        return struct.unpack_from("<I", page, child_base + key_count * 4)[0]

    def _search_leaf(self, page_number: int, hash_bytes: bytes) -> Optional[int]:
        page = self._read_page(page_number)
        _, key_count, _ = LEAF_HEADER_STRUCT.unpack_from(page, 0)

        lo = 0
        hi = key_count - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            offset = LEAF_HEADER_STRUCT.size + mid * LEAF_ENTRY_SIZE
            mid_hash = page[offset : offset + 32]
            if mid_hash == hash_bytes:
                return struct.unpack_from("<Q", page, offset + 32)[0]
            if mid_hash < hash_bytes:
                lo = mid + 1
            else:
                hi = mid - 1
        return None

    def insert(self, hash_bytes: bytes, record_id: int) -> None:
        if not self._writable:
            raise RuntimeError("B+ tree is read-only; open with writable=True")

        inserted, split_info, _, _ = self._insert_recursive(
            self.root_page, hash_bytes, record_id
        )
        if split_info is not None:
            separator_key, new_child_page = split_info
            old_root_page = self.root_page
            new_root_page = self._allocate_page()
            self._write_internal_page(
                new_root_page,
                [old_root_page, new_child_page],
                [separator_key],
            )
            self.root_page = new_root_page

        if inserted:
            self._record_count += 1
            self.record_count = self._record_count
        self._dirty = True

    def delete(self, hash_bytes: bytes) -> bool:
        if not self._writable:
            raise RuntimeError("B+ tree is read-only; open with writable=True")

        deleted, _, _, _ = self._delete_recursive(self.root_page, hash_bytes)
        if not deleted:
            return False

        self._record_count -= 1
        self.record_count = self._record_count
        self._dirty = True
        return True

    def update(self, old_hash: bytes, new_hash: bytes, record_id: int) -> bool:
        """Update index: remove old_hash entry and insert new_hash entry for record_id."""
        if not self._writable:
            raise RuntimeError("B+ tree is read-only; open with writable=True")

        if old_hash == new_hash:
            self.insert(new_hash, record_id)
            return True

        if not self.delete(old_hash):
            return False

        try:
            self.insert(new_hash, record_id)
        except Exception:
            self.insert(old_hash, record_id)
            raise

        return True

    def _find_insert_position(
        self, entries: List[Tuple[bytes, int]], hash_bytes: bytes
    ) -> int:
        lo = 0
        hi = len(entries)
        while lo < hi:
            mid = (lo + hi) // 2
            if entries[mid][0] < hash_bytes:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def _find_entry_index(
        self, entries: List[Tuple[bytes, int]], hash_bytes: bytes
    ) -> Optional[int]:
        index = self._find_insert_position(entries, hash_bytes)
        if index < len(entries) and entries[index][0] == hash_bytes:
            return index
        return None

    def _find_child_index(self, separators: List[bytes], hash_bytes: bytes) -> int:
        for index, separator in enumerate(separators):
            if hash_bytes < separator:
                return index
        return len(separators)

    def _read_leaf_entries(
        self, page_number: int
    ) -> Tuple[List[Tuple[bytes, int]], int]:
        page = self._read_page(page_number)
        _, key_count, next_page = LEAF_HEADER_STRUCT.unpack_from(page, 0)
        entries: List[Tuple[bytes, int]] = []

        for entry_index in range(key_count):
            offset = LEAF_HEADER_STRUCT.size + entry_index * LEAF_ENTRY_SIZE
            hash_bytes, record_id = struct.unpack_from("<32sQ", page, offset)
            entries.append((hash_bytes, int(record_id)))

        return entries, next_page

    def _read_internal_page(self, page_number: int) -> Tuple[List[int], List[bytes]]:
        page = self._read_page(page_number)
        _, key_count = INTERNAL_HEADER_STRUCT.unpack_from(page, 0)
        child_base = INTERNAL_HEADER_STRUCT.size
        key_base = child_base + (key_count + 1) * 4

        children = [
            struct.unpack_from("<I", page, child_base + index * 4)[0]
            for index in range(key_count + 1)
        ]
        separators = [
            bytes(page[key_base + index * 32 : key_base + (index + 1) * 32])
            for index in range(key_count)
        ]
        return children, separators

    def _allocate_page(self) -> int:
        if not self._writable:
            raise RuntimeError("B+ tree is read-only")
        self._file.seek(0, os.SEEK_END)
        page_number = self._file.tell() // PAGE_SIZE
        self._file.write(b"\0" * PAGE_SIZE)
        self._file.flush()
        return page_number

    def _write_leaf_page(
        self, page_number: int, entries: List[Tuple[bytes, int]], next_page: int
    ) -> None:
        page = bytearray(PAGE_SIZE)
        LEAF_HEADER_STRUCT.pack_into(page, 0, NODE_TYPE_LEAF, len(entries), next_page)

        for entry_index, (hash_bytes, record_id) in enumerate(entries):
            offset = LEAF_HEADER_STRUCT.size + entry_index * LEAF_ENTRY_SIZE
            page[offset : offset + 32] = hash_bytes
            struct.pack_into("<Q", page, offset + 32, record_id)

        self._write_page(page_number, page)

    def _write_internal_page(
        self, page_number: int, children: List[int], separators: List[bytes]
    ) -> None:
        if len(children) != len(separators) + 1:
            raise ValueError(
                "internal node must contain one more child than separators"
            )

        page = bytearray(PAGE_SIZE)
        INTERNAL_HEADER_STRUCT.pack_into(page, 0, NODE_TYPE_INTERNAL, len(separators))

        child_base = INTERNAL_HEADER_STRUCT.size
        key_base = child_base + len(children) * 4

        for child_index, child_page in enumerate(children):
            struct.pack_into("<I", page, child_base + child_index * 4, child_page)

        for key_index, separator in enumerate(separators):
            key_offset = key_base + key_index * 32
            page[key_offset : key_offset + 32] = separator

        self._write_page(page_number, page)

    def _read_first_key_from_page(self, page_number: int) -> bytes:
        page = self._read_page(page_number)
        node_type = struct.unpack_from("<B", page, 0)[0]

        if node_type == NODE_TYPE_LEAF:
            _, key_count, _ = LEAF_HEADER_STRUCT.unpack_from(page, 0)
            if key_count == 0:
                raise ValueError("leaf page does not contain a key")
            offset = LEAF_HEADER_STRUCT.size
            return bytes(page[offset : offset + 32])

        if node_type == NODE_TYPE_INTERNAL:
            child_page = struct.unpack_from("<I", page, INTERNAL_HEADER_STRUCT.size)[0]
            return self._read_first_key_from_page(child_page)

        raise ValueError(f"unknown node type in B+ tree: {node_type}")

    def _find_previous_leaf(self, target_page: int) -> Optional[int]:
        page_number = self.first_node_page
        previous_page: Optional[int] = None

        while page_number and page_number != target_page:
            previous_page = page_number
            _, next_page = self._read_leaf_entries(page_number)
            page_number = next_page

        return previous_page if page_number == target_page else None

    def _unlink_leaf(self, leaf_page: int, next_page: int) -> None:
        if self.first_node_page == leaf_page:
            self.first_node_page = next_page
            return

        previous_page = self._find_previous_leaf(leaf_page)
        if previous_page is None:
            raise ValueError(f"failed to find previous leaf for page {leaf_page}")

        entries, _ = self._read_leaf_entries(previous_page)
        self._write_leaf_page(previous_page, entries, next_page)

    def _insert_recursive(
        self, page_number: int, hash_bytes: bytes, record_id: int
    ) -> Tuple[bool, Optional[Tuple[bytes, int]], Optional[bytes], bool]:
        node_type = self._read_node_type(page_number)

        if node_type == NODE_TYPE_LEAF:
            entries, next_page = self._read_leaf_entries(page_number)
            old_first_key = entries[0][0] if entries else None
            insert_pos = self._find_insert_position(entries, hash_bytes)

            if insert_pos < len(entries) and entries[insert_pos][0] == hash_bytes:
                entries[insert_pos] = (hash_bytes, record_id)
                self._write_leaf_page(page_number, entries, next_page)
                new_first_key = entries[0][0] if entries else None
                return False, None, new_first_key, old_first_key != new_first_key

            entries.insert(insert_pos, (hash_bytes, record_id))
            new_first_key = entries[0][0]

            if len(entries) <= self.leaf_capacity:
                self._write_leaf_page(page_number, entries, next_page)
                return True, None, new_first_key, old_first_key != new_first_key

            split_index = len(entries) // 2
            left_entries = entries[:split_index]
            right_entries = entries[split_index:]
            new_page = self._allocate_page()
            self._write_leaf_page(page_number, left_entries, new_page)
            self._write_leaf_page(new_page, right_entries, next_page)
            return (
                True,
                (right_entries[0][0], new_page),
                left_entries[0][0],
                old_first_key != left_entries[0][0],
            )

        if node_type != NODE_TYPE_INTERNAL:
            raise ValueError(f"unknown node type in B+ tree: {node_type}")

        children, separators = self._read_internal_page(page_number)
        old_first_key = self._read_first_key_from_page(page_number)
        child_index = self._find_child_index(separators, hash_bytes)
        inserted, split_info, child_first_key, child_first_changed = (
            self._insert_recursive(children[child_index], hash_bytes, record_id)
        )

        if child_first_changed and child_index > 0 and child_first_key is not None:
            separators[child_index - 1] = child_first_key

        if split_info is not None:
            separator_key, new_child_page = split_info
            separators.insert(child_index, separator_key)
            children.insert(child_index + 1, new_child_page)

        if len(separators) <= self.internal_capacity:
            self._write_internal_page(page_number, children, separators)
            new_first_key = self._read_first_key_from_page(page_number)
            return inserted, None, new_first_key, old_first_key != new_first_key

        split_index = len(separators) // 2
        promoted_key = separators[split_index]
        left_children = children[: split_index + 1]
        right_children = children[split_index + 1 :]
        left_separators = separators[:split_index]
        right_separators = separators[split_index + 1 :]
        new_page = self._allocate_page()

        self._write_internal_page(page_number, left_children, left_separators)
        self._write_internal_page(new_page, right_children, right_separators)

        new_first_key = self._read_first_key_from_page(page_number)
        return (
            inserted,
            (promoted_key, new_page),
            new_first_key,
            old_first_key != new_first_key,
        )

    def _delete_recursive(
        self, page_number: int, hash_bytes: bytes
    ) -> Tuple[bool, bool, Optional[bytes], bool]:
        node_type = self._read_node_type(page_number)

        if node_type == NODE_TYPE_LEAF:
            entries, next_page = self._read_leaf_entries(page_number)
            old_first_key = entries[0][0] if entries else None
            entry_index = self._find_entry_index(entries, hash_bytes)
            if entry_index is None:
                return False, False, old_first_key, False

            del entries[entry_index]

            if page_number == self.root_page:
                self._write_leaf_page(page_number, entries, 0)
                new_first_key = entries[0][0] if entries else None
                return True, False, new_first_key, old_first_key != new_first_key

            if entries:
                self._write_leaf_page(page_number, entries, next_page)
                new_first_key = entries[0][0]
                return True, False, new_first_key, old_first_key != new_first_key

            self._unlink_leaf(page_number, next_page)
            return True, True, None, True

        if node_type != NODE_TYPE_INTERNAL:
            raise ValueError(f"unknown node type in B+ tree: {node_type}")

        children, separators = self._read_internal_page(page_number)
        old_first_key = self._read_first_key_from_page(page_number)
        child_index = self._find_child_index(separators, hash_bytes)
        deleted, remove_child, child_first_key, child_first_changed = (
            self._delete_recursive(children[child_index], hash_bytes)
        )
        if not deleted:
            return False, False, old_first_key, False

        if remove_child:
            del children[child_index]
            if separators:
                separator_index = 0 if child_index == 0 else child_index - 1
                if separator_index < len(separators):
                    del separators[separator_index]
        elif child_first_changed and child_index > 0 and child_first_key is not None:
            separators[child_index - 1] = child_first_key

        if page_number == self.root_page:
            if not children:
                raise ValueError("root internal node lost all children")

            if len(children) == 1:
                self.root_page = children[0]
                if self._read_node_type(self.root_page) == NODE_TYPE_LEAF:
                    self.first_node_page = self.root_page
                new_first_key = (
                    self._read_first_key_from_page(self.root_page)
                    if self._record_count > 1
                    else None
                )
                return True, False, new_first_key, old_first_key != new_first_key

            self._write_internal_page(page_number, children, separators)
            new_first_key = self._read_first_key_from_page(page_number)
            return True, False, new_first_key, old_first_key != new_first_key

        self._write_internal_page(page_number, children, separators)
        new_first_key = self._read_first_key_from_page(page_number)
        return True, False, new_first_key, old_first_key != new_first_key

    def _write_page(self, page_number: int, page: bytes) -> None:
        """Write page data to file."""
        if not self._writable:
            raise RuntimeError("B+ tree is read-only")
        self._file.seek(_page_offset(page_number))
        self._file.write(page)
        self._file.flush()

    def sync(self) -> None:
        """Sync header to disk if any modifications were made."""
        if not self._writable or not self._dirty:
            return

        # Update header with new record count
        header = HEADER_STRUCT.pack(
            HEADER_MAGIC,
            self.page_size,
            self.root_page,
            self._record_count,
            self.leaf_capacity,
            self.internal_capacity,
            self.first_node_page,
        )
        self._file.seek(0)
        self._file.write(header)
        self._file.flush()
        self._dirty = False

    def close(self) -> None:
        if hasattr(self, "_dirty") and self._dirty:
            self.sync()
        try:
            self._file.close()
        except Exception:
            pass


def build_bplus_tree(
    db_path: Optional[str] = None,
    out_path: Optional[str] = None,
) -> None:
    start_time = time.perf_counter()
    builder = BPlusTreeBuilder(db_path=db_path, out_path=out_path)
    builder.build()
    elapsed = time.perf_counter() - start_time
    print(f"B+ tree build completed in {elapsed:.2f} seconds", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the B+ tree hash index.")
    parser.parse_args()
    build_bplus_tree()


if __name__ == "__main__":
    main()
