import os
import tempfile
import threading
import unittest
from unittest.mock import patch

import emulator.storage.engine as database_module
from emulator.utils import compute_hash_for


class TestDbEngine(unittest.TestCase):
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

        self.db = database_module.DbEngine()
        self.db.ensure_capacity(32)
        self.db.populate_range(0, 32)

    def test_read_record_returns_expected_row(self):
        record_id = 7

        id_read, name, hash = self.db.read_record(record_id)

        self.assertEqual(id_read, record_id)
        self.assertEqual(name, "aaaah")
        self.assertEqual(hash, compute_hash_for(record_id, name.encode("ascii")).hex())

    def test_query_by_hash_returns_matching_record(self):
        record_id = 19
        _, expected_name, hash_hex = self.db.read_record(record_id)
        result = self.db.query_by_hash(hash_hex)

        self.assertEqual(result, (record_id, expected_name))

    def test_query_by_hash_returns_none_for_unknown_hash(self):
        missing_hash = b"\x00" * 32

        self.assertIsNone(self.db.query_by_hash(missing_hash.hex()))

    def test_multiple_connections_can_read_concurrently(self):
        exceptions = []
        start_event = threading.Event()

        def worker(record_id: int):
            local_db = database_module.DbEngine()
            try:
                start_event.wait(timeout=2)
                id_read, name, hash_hex = local_db.read_record(record_id)
                result = local_db.query_by_hash(hash_hex)
                self.assertEqual(id_read, record_id)
                self.assertEqual(result, (record_id, name))
            except Exception as exc:
                exceptions.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()

        start_event.set()

        for thread in threads:
            thread.join()

        self.assertEqual(exceptions, [])


if __name__ == "__main__":
    unittest.main()
