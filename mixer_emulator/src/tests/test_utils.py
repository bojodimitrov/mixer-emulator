import os
import tempfile
import unittest
from unittest.mock import patch

import emulator.storage.database as database_module


class TestFileDB(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test.db")
        self.index_path = os.path.join(self.temp_dir.name, "test.idx")

        db_path_patch = patch.object(database_module, "DEFAULT_DB_PATH", self.db_path)
        index_path_patch = patch.object(
            database_module, "DEFAULT_INDEX_PATH", self.index_path
        )
        self.addCleanup(db_path_patch.stop)
        self.addCleanup(index_path_patch.stop)
        db_path_patch.start()
        index_path_patch.start()

        self.addCleanup(self.temp_dir.cleanup)

        self.db = database_module.FileDB()
        self.db.ensure_capacity(32)
        self.db.populate_range(0, 32)

    def test_read_record_returns_expected_row(self):
        record_id = 7

        id_read, name, hash_bytes = self.db.read_record(record_id)

        self.assertEqual(id_read, record_id)
        self.assertEqual(name, "aaaah")
        self.assertEqual(
            hash_bytes, self.db.compute_hash_for(record_id, name.encode("ascii"))
        )

    def test_query_by_hash_returns_matching_record(self):
        record_id = 19
        _, expected_name, hash_bytes = self.db.read_record(record_id)
        result = self.db.query_by_hash(hash_bytes)

        self.assertEqual(result, (record_id, expected_name))

    def test_query_by_hash_returns_none_for_unknown_hash(self):
        missing_hash = b"\x00" * 32

        self.assertIsNone(self.db.query_by_hash(missing_hash))


if __name__ == "__main__":
    unittest.main()
