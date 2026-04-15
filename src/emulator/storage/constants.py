from pathlib import Path


RECORD_STRUCT = "<Q5s32s"  # id:uint64 | name:5s | hash:32s
RECORD_SIZE = __import__("struct").calcsize(RECORD_STRUCT)

ID_FMT = "<Q"
NAME_LEN = 5
HASH_LEN = 32

DB_RECORD_SIZE = 8 + NAME_LEN + HASH_LEN
DB_HASH_OFFSET = 8 + NAME_LEN

INDEX_STRUCT = "<32sQ"  # hash:32s | id:uint64
INDEX_RECORD_SIZE = __import__("struct").calcsize(INDEX_STRUCT)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DB_DIR = PROJECT_ROOT / "db"

DEFAULT_DB_PATH = str(DB_DIR / "mixer_emulator_bin.db")
DEFAULT_BPLUS_INDEX_PATH = str(DB_DIR / "mixer_emulator.bpt")

# ── Sharding ──────────────────────────────────────────────────────────────────
SHARD_COUNT = 4
GSI_INDEX_PATH = str(DB_DIR / "gsi" / "hash_index.bpt")


def db_shard_path(shard_index: int) -> str:
    """Return the DB file path for the given shard index."""
    return str(DB_DIR / f"shard_{shard_index}" / "records.db")
