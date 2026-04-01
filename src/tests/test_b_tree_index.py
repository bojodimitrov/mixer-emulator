import os
import tempfile
import unittest
from unittest.mock import patch

import emulator.storage.b_tree_index as btree_module
import emulator.storage.engine as database_module
from emulator.utils import compute_hash_for


class TestBPlusTreeIndex(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test.db")
        self.bplus_index_path = os.path.join(self.temp_dir.name, "test.bpt")

        patches = [
            patch.object(database_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(
                database_module, "DEFAULT_BPLUS_INDEX_PATH", self.bplus_index_path
            ),
            patch.object(btree_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(
                btree_module, "DEFAULT_BPLUS_INDEX_PATH", self.bplus_index_path
            ),
        ]

        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)

        self.addCleanup(self.temp_dir.cleanup)

        self.db = database_module.DbEngine(
            lookup_strategy=database_module.DbEngine.STRATEGY_BPLUS
        )
        self.db.ensure_capacity(128)
        self.db.populate_range(0, 128)
        # B+ tree builder now reads directly from database, no need to build sorted index first
        btree_module.build_bplus_tree()

    def test_bplus_query_returns_matching_id(self):
        record_id, _, hash_bytes = self.db.read_record(91)

        with btree_module.BPlusTreeIndex() as index:
            self.assertEqual(index.query_by_hash(hash_bytes), record_id)

    def test_database_query_by_hash_bplus_returns_matching_record(self):
        record_id, expected_name, hash_bytes = self.db.read_record(17)

        self.assertEqual(
            self.db.query_by_hash(hash_bytes),
            (record_id, expected_name),
        )

    def test_bplus_query_returns_none_for_missing_hash(self):
        with btree_module.BPlusTreeIndex() as index:
            self.assertIsNone(index.query_by_hash(b"\xff" * 32))

    def test_bplus_update_removes_old_hash_and_adds_new(self):
        """Test that B-tree update operation removes old hash and adds new."""
        record_id, _, old_hash = self.db.read_record(42)

        # Verify old hash is in the index
        with btree_module.BPlusTreeIndex() as index:
            self.assertEqual(index.query_by_hash(old_hash), record_id)

        # Update the record with a new name
        new_name = "zzzzz"
        self.db.update_record_with_bplus_index(record_id, new_name)

        # Read updated record to get new hash
        _, _, new_hash = self.db.read_record(record_id)
        old_hash_should_not_match = old_hash != new_hash  # Verify hash actually changed
        self.assertTrue(old_hash_should_not_match)

        # Verify old hash no longer returns anything
        with btree_module.BPlusTreeIndex() as index:
            self.assertIsNone(index.query_by_hash(old_hash))

        # Verify new hash returns the record ID
        with btree_module.BPlusTreeIndex() as index:
            self.assertEqual(index.query_by_hash(new_hash), record_id)

    def test_database_query_by_hash_bplus_after_update(self):
        """Test that query_by_hash_bplus works correctly after update."""
        record_id = 55
        old_name, old_hash = self.db.read_record(record_id)[1:3]

        new_name = "abbbb"
        self.db.update_record_with_bplus_index(record_id, new_name)

        # Query by new hash should return the record with new name
        result = self.db.query_by_hash(
            compute_hash_for(record_id, new_name.encode("ascii"))
        )
        self.assertIsNotNone(result)
        if result is not None:
            self.assertEqual(result[0], record_id)
            self.assertEqual(result[1], new_name)

        # Query by old hash should return None
        result_old = self.db.query_by_hash(old_hash)
        self.assertIsNone(result_old)

    def test_bplus_update_rebuilds_tree_with_small_capacities(self):
        record_id, _, old_hash = self.db.read_record(10)

        with patch.object(btree_module, "LEAF_CAPACITY", 2), patch.object(
            btree_module, "INTERNAL_CAPACITY", 2
        ):
            btree_module.build_bplus_tree()
            self.db.update_record_with_bplus_index(record_id, "zzzzz")

        _, _, new_hash = self.db.read_record(record_id)

        with btree_module.BPlusTreeIndex() as index:
            self.assertIsNone(index.query_by_hash(old_hash))
            self.assertEqual(index.query_by_hash(new_hash), record_id)

            for check_id in (0, 1, 2, 63, 64, 127):
                expected_id, _, hash_bytes = self.db.read_record(check_id)
                self.assertEqual(index.query_by_hash(hash_bytes), expected_id)

    def test_bplus_direct_insert_and_delete_with_small_capacities(self):
        inserted_hash = b"\x00" * 32
        inserted_id = 999_999

        with patch.object(btree_module, "LEAF_CAPACITY", 2), patch.object(
            btree_module, "INTERNAL_CAPACITY", 2
        ):
            btree_module.build_bplus_tree()

            with btree_module.BPlusTreeIndex(writable=True) as index:
                self.assertIsNone(index.query_by_hash(inserted_hash))
                index.insert(inserted_hash, inserted_id)

            with btree_module.BPlusTreeIndex() as index:
                self.assertEqual(index.query_by_hash(inserted_hash), inserted_id)
                for check_id in (0, 1, 2, 63, 64, 127):
                    expected_id, _, hash_bytes = self.db.read_record(check_id)
                    self.assertEqual(index.query_by_hash(hash_bytes), expected_id)

            with btree_module.BPlusTreeIndex(writable=True) as index:
                self.assertTrue(index.delete(inserted_hash))

            with btree_module.BPlusTreeIndex() as index:
                self.assertIsNone(index.query_by_hash(inserted_hash))
                for check_id in (0, 1, 2, 63, 64, 127):
                    expected_id, _, hash_bytes = self.db.read_record(check_id)
                    self.assertEqual(index.query_by_hash(hash_bytes), expected_id)


if __name__ == "__main__":
    unittest.main()
