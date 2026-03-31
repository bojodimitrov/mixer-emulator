import os
import struct
import mmap
import hashlib
from typing import Optional, Tuple
from .constants import RECORD_STRUCT, RECORD_SIZE, DB_HASH_OFFSET, DEFAULT_DB_PATH, DB_RECORD_SIZE, DEFAULT_INDEX_PATH

class FileDB:
    """
    Fixed-size-record file DB using mmap + struct.

    Record format (per row):
      - id: unsigned 64-bit (Q)
      - name: exactly 5 ASCII lower-case letters (5s)
      - hash: sha256(id:name) digest (32s)

    Hash algorithm used: sha256(f"{id}:{name}".encode("utf-8")).digest()

    Notes:
    - Rows are addressed by id (0-based). The file size determines number of rows.
    - You can pre-allocate capacity with ensure_capacity(capacity).
    - populate_range will write computed name/hash for ids in [start, end).
    - query_by_hash scans the file and returns (id, name) on match or None.
    - Generating all 11_881_376 rows will produce a ~535 MB file; populating will take time.
    """

    def __init__(self):
        os.makedirs(os.path.dirname(DEFAULT_DB_PATH) or ".", exist_ok=True)
        # ensure file exists
        open(DEFAULT_DB_PATH, "a").close()

    @staticmethod
    def _id_to_name(idx: int) -> bytes:
        result = bytearray(5)
        n = idx
        for i in range(4, -1, -1):
            result[i] = ord('a') + (n % 26)
            n //= 26
        return bytes(result)

    @staticmethod
    def compute_hash_for(id_: int, name: bytes) -> bytes:
        # client/db must use the same scheme
        s = f"{id_}:{name.decode('ascii')}".encode("utf-8")
        return hashlib.sha256(s).digest()

    def record_count(self) -> int:
        size = os.path.getsize(DEFAULT_DB_PATH)
        return size // RECORD_SIZE

    def ensure_capacity(self, capacity: int) -> None:
        """
        Ensure file has space for `capacity` records. If file is smaller it will be
        truncated/expanded to capacity * RECORD_SIZE (zero-filled).
        """
        target = capacity * RECORD_SIZE

        with open(DEFAULT_DB_PATH, "ab") as f:
            cur = f.tell()
        
        cur = os.path.getsize(DEFAULT_DB_PATH)
        if cur < target:
            with open(DEFAULT_DB_PATH, "r+b") as f:
                f.truncate(target)

    def update_record(self, id_: int, name_str: str) -> None:
        """
        Update the name (second column) for the given id with a 5-letter lowercase word
        and recompute/write the corresponding hash. Raises IndexError if id out of range
        or ValueError for invalid name.
        """
        # validate name
        if not isinstance(name_str, str) or len(name_str) != 5 or not name_str.isalpha() or not name_str.islower():
            raise ValueError("name_str must be a 5-letter lowercase ASCII word")

        # ensure id is within file bounds
        file_size = os.path.getsize(DEFAULT_DB_PATH)
        if id_ * RECORD_SIZE + RECORD_SIZE > file_size:
            raise IndexError("id out of range")

        name = name_str.encode("ascii")
        hashb = self.compute_hash_for(id_, name)
        packed = struct.pack(RECORD_STRUCT, id_, name, hashb)

        with open(DEFAULT_DB_PATH, "r+b") as f:
            f.seek(id_ * RECORD_SIZE)
            f.write(packed)
            f.flush()

    def populate_range(self, start: int, end: int, progress_callback: Optional[callable] = None) -> None: # type: ignore
        """
        Fill records for ids in [start, end). Make sure ensure_capacity(end) was called.
        Use progress_callback(i, total) if provided.
        """
        total = end - start
        with open(DEFAULT_DB_PATH, "r+b") as f:
            mm = mmap.mmap(f.fileno(), 0)
            try:
                for i, id_ in enumerate(range(start, end), 1):
                    name = self._id_to_name(id_)
                    hashb = self.compute_hash_for(id_, name)
                    packed = struct.pack(RECORD_STRUCT, id_, name, hashb)
                    off = id_ * RECORD_SIZE
                    mm[off: off + RECORD_SIZE] = packed
                    if progress_callback and (i % 10000 == 0 or i == total):
                        progress_callback(i, total)
            finally:
                mm.close()

    def read_record(self, id_: int) -> Tuple[int, str, bytes]:
        """
        Read record at given id. Returns (id, name_str, hash_bytes).
        Raises IndexError if id is out of bounds.
        """
        file_size = os.path.getsize(DEFAULT_DB_PATH)
        if id_ * RECORD_SIZE + RECORD_SIZE > file_size:
            raise IndexError("id out of range")
        
        with open(DEFAULT_DB_PATH, "rb") as f:
            f.seek(id_ * RECORD_SIZE)
            data = f.read(RECORD_SIZE)
        
        id_read, name_b, hashb = struct.unpack(RECORD_STRUCT, data)
        return id_read, name_b.decode("ascii"), hashb

    def query_by_hash(self, hash_bytes: bytes) -> Optional[Tuple[int, str]]:
        """
        Scan the file for a matching hash. Returns (id, name) on first match, else None.
        Note: scanning large file is linear-time; you'll later add an index.
        """
        file_size = os.path.getsize(DEFAULT_DB_PATH)

        if file_size == 0:
            return None
        
        with open(DEFAULT_DB_PATH, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                for off in range(0, len(mm), RECORD_SIZE):
                    # hash is at offset + 8 + 5
                    hb = mm[off + DB_HASH_OFFSET: off + DB_RECORD_SIZE]
                    if hb == hash_bytes:
                        # unpack id and name
                        id_, name_b, _ = struct.unpack_from(RECORD_STRUCT, mm, off)
                        return int(id_), name_b.decode("ascii")
                return None
            finally:
                mm.close()

    def query_by_hash_index(self, hash_bytes: bytes) -> Optional[Tuple[int, str]]:
        """
        Lookup by hash using the on-disk HashIndex for fast exact-match lookups.
        Returns (id, name) on match, else None.
        Raises FileNotFoundError if the index file is missing.
        """
        from .sorted_index import HashIndex

        if not os.path.exists(DEFAULT_INDEX_PATH):
            raise FileNotFoundError(f"index file not found: {DEFAULT_INDEX_PATH}")

        with HashIndex() as idx:
            id_ = idx.query_by_hash(hash_bytes)
            if id_ is None:
                return None
            try:
                id_read, name, _ = self.read_record(id_)
            except Exception:
                return None
            return id_read, name