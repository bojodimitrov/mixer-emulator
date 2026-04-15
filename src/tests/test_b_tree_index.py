import os
import tempfile
import unittest
from unittest.mock import patch

import emulator.storage.b_tree_index as btree_module
import emulator.storage.engine as database_module
from emulator.utils import compute_hash_for


def _update_db_and_index(db, record_id: int, new_name: str) -> None:
    """Update the DB record and then propagate the hash change to the B+ index.

    This mirrors the two-step update that the microservice performs in the
    sharded architecture: shard engine writes the record, GSI is updated
    separately (here inline for test simplicity).
    """
    _, _, old_hash_hex = db.read_record(record_id)
    db.command_update_record(record_id, new_name)
    _, _, new_hash_hex = db.read_record(record_id)
    old_hash_bytes = bytes.fromhex(old_hash_hex)
    new_hash_bytes = bytes.fromhex(new_hash_hex)
    with btree_module.BPlusTreeIndex(writable=True) as idx:
        idx.update(old_hash_bytes, new_hash_bytes, record_id)


class TestBPlusTreeIndex(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test.db")
        self.bplus_index_path = os.path.join(self.temp_dir.name, "test.bpt")

        patches = [
            patch.object(database_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(
                btree_module, "DEFAULT_BPLUS_INDEX_PATH", self.bplus_index_path
            ),
            patch.object(
                btree_module, "GSI_INDEX_PATH", self.bplus_index_path
            ),
        ]

        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)

        self.addCleanup(self.temp_dir.cleanup)

        self.db = database_module.DbEngine()
        self.db.ensure_capacity(128)
        self.db.populate_range(0, 128)
        # B+ tree builder now reads directly from database, no need to build sorted index first
        btree_module.build_bplus_tree(db_path=self.db_path)

    def test_bplus_query_returns_matching_id(self):
        record_id, _, hash = self.db.read_record(91)

        with btree_module.BPlusTreeIndex() as index:
            self.assertEqual(index.query_by_hash(hash), record_id)

    def test_bplus_query_returns_record_via_index(self):
        record_id, expected_name, hash_hex = self.db.read_record(17)

        with btree_module.BPlusTreeIndex() as index:
            found_id = index.query_by_hash(hash_hex)

        self.assertIsNotNone(found_id)
        assert found_id is not None
        found_id_read, found_name, _ = self.db.read_record(found_id)
        self.assertEqual((found_id_read, found_name), (record_id, expected_name))

    def test_bplus_query_returns_none_for_missing_hash(self):
        with btree_module.BPlusTreeIndex() as index:
            self.assertIsNone(index.query_by_hash((b"\xff" * 32).hex()))

    def test_bplus_update_removes_old_hash_and_adds_new(self):
        """Test that B-tree update operation removes old hash and adds new."""
        record_id, _, old_hash = self.db.read_record(42)

        # Verify old hash is in the index
        with btree_module.BPlusTreeIndex() as index:
            self.assertEqual(index.query_by_hash(old_hash), record_id)

        # Update the DB record then propagate the hash change to the index.
        new_name = "zzzzz"
        _update_db_and_index(self.db, record_id, new_name)

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

    def test_bplus_index_reflects_update(self):
        """Test that the GSI correctly reflects a DB record update."""
        record_id = 55
        _, old_hash = self.db.read_record(record_id)[1:3]

        new_name = "abbbb"
        _update_db_and_index(self.db, record_id, new_name)

        new_hash_hex = compute_hash_for(record_id, new_name.encode("ascii")).hex()

        # Index resolves new hash to the correct record.
        with btree_module.BPlusTreeIndex() as index:
            found_id = index.query_by_hash(new_hash_hex)
        self.assertIsNotNone(found_id)
        self.assertEqual(found_id, record_id)

        # Index no longer resolves the old hash.
        with btree_module.BPlusTreeIndex() as index:
            self.assertIsNone(index.query_by_hash(old_hash))

    def test_bplus_update_rebuilds_tree_with_small_capacities(self):
        record_id, _, old_hash = self.db.read_record(10)

        with patch.object(btree_module, "LEAF_CAPACITY", 2), patch.object(
            btree_module, "INTERNAL_CAPACITY", 2
        ):
            btree_module.build_bplus_tree(db_path=self.db_path)
            _update_db_and_index(self.db, record_id, "zzzzz")

        _, _, new_hash = self.db.read_record(record_id)

        with btree_module.BPlusTreeIndex() as index:
            self.assertIsNone(index.query_by_hash(old_hash))
            self.assertEqual(index.query_by_hash(new_hash), record_id)

            for check_id in (0, 1, 2, 63, 64, 127):
                expected_id, _, hash_hex = self.db.read_record(check_id)
                self.assertEqual(index.query_by_hash(hash_hex), expected_id)

    def test_bplus_direct_insert_and_delete_with_small_capacities(self):
        inserted_hash = b"\x00" * 32
        inserted_id = 999_999

        with patch.object(btree_module, "LEAF_CAPACITY", 2), patch.object(
            btree_module, "INTERNAL_CAPACITY", 2
        ):
            btree_module.build_bplus_tree(db_path=self.db_path)

            with btree_module.BPlusTreeIndex(writable=True) as index:
                self.assertIsNone(index.query_by_hash(inserted_hash.hex()))
                index.insert(inserted_hash, inserted_id)

            with btree_module.BPlusTreeIndex() as index:
                self.assertEqual(index.query_by_hash(inserted_hash.hex()), inserted_id)
                for check_id in (0, 1, 2, 63, 64, 127):
                    expected_id, _, hash_hex = self.db.read_record(check_id)
                    self.assertEqual(index.query_by_hash(hash_hex), expected_id)

            with btree_module.BPlusTreeIndex(writable=True) as index:
                self.assertTrue(index.delete(inserted_hash))

            with btree_module.BPlusTreeIndex() as index:
                self.assertIsNone(index.query_by_hash(inserted_hash.hex()))
                for check_id in (0, 1, 2, 63, 64, 127):
                    expected_id, _, hash_hex = self.db.read_record(check_id)
                    self.assertEqual(index.query_by_hash(hash_hex), expected_id)


if __name__ == "__main__":
    unittest.main()


    def test_bplus_query_returns_matching_id(self):
        record_id, _, hash = self.db.read_record(91)

        with btree_module.BPlusTreeIndex() as index:
            self.assertEqual(index.query_by_hash(hash), record_id)

    def test_database_query_by_hash_bplus_returns_matching_record(self):
        record_id, expected_name, hash_hex = self.db.read_record(17)

        self.assertEqual(
            self.db.query_by_hash(hash_hex),
            (record_id, expected_name),
        )

    def test_bplus_query_returns_none_for_missing_hash(self):
        with btree_module.BPlusTreeIndex() as index:
            self.assertIsNone(index.query_by_hash((b"\xff" * 32).hex()))

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
            compute_hash_for(record_id, new_name.encode("ascii")).hex()
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
                expected_id, _, hash_hex = self.db.read_record(check_id)
                self.assertEqual(index.query_by_hash(hash_hex), expected_id)

    def test_bplus_direct_insert_and_delete_with_small_capacities(self):
        inserted_hash = b"\x00" * 32
        inserted_id = 999_999

        with patch.object(btree_module, "LEAF_CAPACITY", 2), patch.object(
            btree_module, "INTERNAL_CAPACITY", 2
        ):
            btree_module.build_bplus_tree()

            with btree_module.BPlusTreeIndex(writable=True) as index:
                self.assertIsNone(index.query_by_hash(inserted_hash.hex()))
                index.insert(inserted_hash, inserted_id)

            with btree_module.BPlusTreeIndex() as index:
                self.assertEqual(index.query_by_hash(inserted_hash.hex()), inserted_id)
                for check_id in (0, 1, 2, 63, 64, 127):
                    expected_id, _, hash_hex = self.db.read_record(check_id)
                    self.assertEqual(index.query_by_hash(hash_hex), expected_id)

            with btree_module.BPlusTreeIndex(writable=True) as index:
                self.assertTrue(index.delete(inserted_hash))

            with btree_module.BPlusTreeIndex() as index:
                self.assertIsNone(index.query_by_hash(inserted_hash.hex()))
                for check_id in (0, 1, 2, 63, 64, 127):
                    expected_id, _, hash_hex = self.db.read_record(check_id)
                    self.assertEqual(index.query_by_hash(hash_hex), expected_id)

    def test_bplus_update_rolls_back_on_non_runtime_error(self):
        record_id = 21
        id_before, name_before, old_hash = self.db.read_record(record_id)
        self.assertEqual(id_before, record_id)

        with patch.object(
            btree_module.BPlusTreeIndex,
            "update",
            side_effect=ValueError("forced failure"),
        ):
            with self.assertRaisesRegex(ValueError, "forced failure"):
                self.db.update_record_with_bplus_index(record_id, "zzzzz")

        # DB row must be fully rolled back.
        id_after, name_after, hash_after = self.db.read_record(record_id)
        self.assertEqual(id_after, record_id)
        self.assertEqual(name_after, name_before)
        self.assertEqual(hash_after, old_hash)

        # Index must still resolve old hash and not include the would-be new hash.
        with btree_module.BPlusTreeIndex() as index:
            self.assertEqual(index.query_by_hash(old_hash), record_id)
            new_hash = compute_hash_for(record_id, b"zzzzz").hex()
            self.assertIsNone(index.query_by_hash(new_hash))


if __name__ == "__main__":
    unittest.main()
