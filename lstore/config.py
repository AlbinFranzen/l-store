# global variables
PAGE_SIZE = 32 # 4096 bytes
PAGE_RECORD_SIZE = 4 # 512 records/page
PAGE_RANGE_SIZE = 2 # 16 Base pages/page range
MERGE_THRESH = PAGE_RECORD_SIZE # PAGE_RECORD_SIZE * PAGE_RANGE_SIZE * 4 updates/merge
POOL_SIZE = 5  # 1024 pages/bufferpool

# record meta-data columns
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
