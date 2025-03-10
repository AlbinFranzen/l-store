# global variables
PAGE_SIZE = 4096 # 4096 bytes
PAGE_RECORD_SIZE = 512 # 512 records/page
PAGE_RANGE_SIZE = 16 # Base pages/page range
MERGE_THRESH = PAGE_RECORD_SIZE * PAGE_RANGE_SIZE * 4 # updates/merge
POOL_SIZE = 1024 #pages/bufferpool

# record meta-data columns
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
