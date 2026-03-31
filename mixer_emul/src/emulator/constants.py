RECORD_STRUCT = "<Q5s32s"   # id:uint64 | name:5s | hash:32s
RECORD_SIZE = __import__("struct").calcsize(RECORD_STRUCT)

ID_FMT = "<Q"
NAME_LEN = 5
HASH_LEN = 32

DB_RECORD_SIZE = 8 + NAME_LEN + HASH_LEN
DB_HASH_OFFSET = 8 + NAME_LEN

INDEX_STRUCT = "<32sQ"      # hash:32s | id:uint64
INDEX_RECORD_SIZE = __import__("struct").calcsize(INDEX_STRUCT)

DEFAULT_DB_PATH = "./db/mixer_emul_bin.db"
DEFAULT_INDEX_PATH = "./db/mixer_emul.idx"