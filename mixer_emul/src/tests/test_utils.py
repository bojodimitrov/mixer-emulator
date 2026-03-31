import random
import unittest

from mixer_emul.src.emulator.database import FileDB

class TestUtils(unittest.TestCase):

    def test_db(self):
        db = FileDB()
        # Test case for the helper_function
        id, name, hashb = db.read_record(random.randint(0, db.record_count() - 1))
        h_bytes = bytes.fromhex(hashb.hex())
        result = db.query_by_hash(h_bytes)

        self.assertIsNotNone(result, "query_by_hash returned None for hash: {}".format(h_bytes.hex()))
        # Ensure result is an iterable of two items before unpacking
        if result is None or not isinstance(result, (list, tuple)) or len(result) != 2:
            self.fail("query_by_hash returned unexpected value for hash {}: {!r}".format(h_bytes.hex(), result))
    
        id_result, name_result = result

        self.assertEqual(id_result, id, "query_by_hash returned incorrect id for hash: {}".format(h_bytes.hex()))
        self.assertEqual(name_result, name, "query_by_hash returned incorrect name for hash: {}".format(h_bytes.hex()))

if __name__ == '__main__':
    unittest.main()