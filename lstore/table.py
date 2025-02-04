from lstore.index import Index
from time import time
from lstore.page_range import PageRange

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, indirection, rid, key, time_stamp, schema_encoding, columns):
        self.rid = rid
        self.indirection = indirection
        self.key = key
        self.time_stamp = time_stamp
        self.schema_encoding = schema_encoding
        self.columns = columns

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_ranges = [PageRange()]
        self.page_directory = {}
        self.index = Index(self)
        pass
        

    def __merge(self):
        print("merge is happening")
        pass
