import os
import struct
import tempfile
import unittest
from unittest.mock import patch

import emulator.storage.database as database_module
import emulator.storage.sorted_index as sorted_index_module


class TestSortedIndex(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test.db")
        self.index_path = os.path.join(self.temp_dir.name, "test.idx")

        patches = [
            patch.object(database_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(database_module, "DEFAULT_INDEX_PATH", self.index_path),
            patch.object(sorted_index_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(sorted_index_module, "DEFAULT_INDEX_PATH", self.index_path),
        ]

        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)

        self.addCleanup(self.temp_dir.cleanup)

        self.db = database_module.FileDB(
            lookup_strategy=database_module.FileDB.STRATEGY_SORTED
        )
        self.db.ensure_capacity(128)
        self.db.populate_range(0, 128)
        sorted_index_module.build_index(chunk_size=32)

    def test_hash_index_query_returns_matching_id(self):
        record_id, _, hash_bytes = self.db.read_record(63)

        with sorted_index_module.HashIndex() as index:
            self.assertEqual(index.query_by_hash(hash_bytes), record_id)

    def test_database_query_by_hash_index_returns_matching_record(self):
        record_id, expected_name, hash_bytes = self.db.read_record(24)

        self.assertEqual(
            self.db.query_by_hash(hash_bytes),
            (record_id, expected_name),
        )

    def test_built_index_file_is_sorted_by_hash(self):
        entries = []
        with open(self.index_path, "rb") as index_file:
            while True:
                data = index_file.read(sorted_index_module.INDEX_RECORD_SIZE)
                if not data:
                    break
                entries.append(struct.unpack(sorted_index_module.INDEX_STRUCT, data))

        self.assertEqual(len(entries), 128)
        self.assertEqual(entries, sorted(entries, key=lambda entry: entry[0]))

    def test_hash_index_query_returns_none_for_unknown_hash(self):
        with sorted_index_module.HashIndex() as index:
            self.assertIsNone(index.query_by_hash(b"\xff" * 32))

    def test_hash_index_writable_insert_and_delete(self):
        inserted_hash = b"\x00" * 32
        inserted_id = 999_999

        with sorted_index_module.HashIndex(writable=True) as index:
            self.assertIsNone(index.query_by_hash(inserted_hash))
            index.insert(inserted_hash, inserted_id)

        with sorted_index_module.HashIndex() as index:
            self.assertEqual(index.query_by_hash(inserted_hash), inserted_id)

        entries = []
        with open(self.index_path, "rb") as index_file:
            while True:
                data = index_file.read(sorted_index_module.INDEX_RECORD_SIZE)
                if not data:
                    break
                entries.append(struct.unpack(sorted_index_module.INDEX_STRUCT, data))

        self.assertEqual(len(entries), 129)
        self.assertEqual(entries, sorted(entries, key=lambda entry: entry[0]))

        with sorted_index_module.HashIndex(writable=True) as index:
            self.assertTrue(index.delete(inserted_hash))
            self.assertFalse(index.delete(inserted_hash))

        with sorted_index_module.HashIndex() as index:
            self.assertIsNone(index.query_by_hash(inserted_hash))

    def test_database_update_record_dispatch_updates_sorted_index(self):
        record_id = 42
        old_hash = self.db.read_record(record_id)[2]
        new_name = "zzzzz"

        self.assertTrue(self.db.update_record(record_id, new_name))

        id_read, name_read, new_hash = self.db.read_record(record_id)
        self.assertEqual(id_read, record_id)
        self.assertEqual(name_read, new_name)
        self.assertNotEqual(old_hash, new_hash)

        with sorted_index_module.HashIndex() as index:
            self.assertIsNone(index.query_by_hash(old_hash))
            self.assertEqual(index.query_by_hash(new_hash), record_id)


if __name__ == "__main__":
    unittest.main()
