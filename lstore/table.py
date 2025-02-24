import os
from lstore.index import Index
from time import time
# from lstore.page_range import PageRange
from lstore.bufferpool import BufferPool
from lstore.page import Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, indirection, rid, time_stamp, schema_encoding, columns):
        self.indirection = indirection
        self.rid = rid
        self.time_stamp = time_stamp
        self.schema_encoding = schema_encoding
        self.columns = columns

    def __repr__(self):
        return f"indirection: {self.indirection}  |  rid: {self.rid}  |  time_stamp: {self.time_stamp}  |  schema_encoding: {self.schema_encoding}  |  columns: {self.columns}"

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        # Table metadata
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.path = os.path.join("database", name)
        self.page_directory = {}
        self.index = Index(self)
        self.bufferpool = BufferPool(self.path)    
        self._init_page_range_storage()
        self.last_path = os.path.join(self.path, "pagerange_0/base/page_0")
        
    def _init_page_range_storage(self):
        """Creates initial page range directory and storage"""
        base_path = os.path.join(self.path, "pagerange_0/base")
        tail_path = os.path.join(self.path, "pagerange_0/tail")
        
        os.makedirs(base_path, exist_ok=True)
        os.makedirs(tail_path, exist_ok=True)
        
        for path in [base_path, tail_path]:
            with open(os.path.join(path, "page_0"), 'wb') as f:
                f.write(Page().serialize())
        
    def __repr__(self):
        return f"Name: {self.name}\nKey: {self.key}\nNum columns: {self.num_columns}\nPage_ranges: {self.page_ranges}\nPage_directory: {self.page_directory}\nindex: {self.index}"

    def __merge(self):
        print("merge is happening")
        pass
