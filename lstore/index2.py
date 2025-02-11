import bisect
import os
from lstore.bplustree.tree import BPlusTree

"""
A data structure holding indices for various columns of a table.
Key column should be indexed by default; other columns can be indexed as well.
Indices are usually B+Trees, but other data structures can be used.
"""
class Index:

    def __init__(self, table):
        # One index for each column in the table
        self.indices = [None] * table.num_columns
        self.table_name = table.name
        
        # Create an insert cache per column.
        # Each cache is a list of tuples: (key, value)
        self.insert_cache = {col: [] for col in range(table.num_columns)}
        
        for col in range(table.num_columns):
            self.create_index(col)

    def __repr__(self):
        return f"indices: {self.indices}"

    def flush_cache(self):
        """
        Flush the cached insertions for all columns.
        For each column with pending cached entries, sort them (ascending order by key)
        and call the B+Tree batch_insert method, then clear the cache.
        """
        for col, cache in self.insert_cache.items():
            if cache:
                # Note: batch_insert requires the iterable to be sorted in ascending order.
                sorted_cache = sorted(cache, key=lambda tup: tup[0])
                # If your B+Tree expects all keys to be greater than existing keys,
                # ensure that this is the case.
                self.indices[col].batch_insert(sorted_cache)
                self.insert_cache[col] = []  # Clear the cache

    def locate(self, column, value):
        """
        Returns the location of the record with the given value in the specified column.
        Before performing the lookup, flush any pending cached inserts.
        """
        # Flush pending inserts so that the B+Tree is up-to-date.
        self.flush_cache()

        if value is None:
            return False
        if self.indices[column]:  # Ensure the B+Tree exists for the column
            column_index = self.indices[column]
            try:
                # Assuming the tree returns a bytes value for the RID
                if column_index[value] is not None:
                    return column_index[value].decode('utf-8')
            except (IndexError, KeyError):
                return False  # Return False if no matching record is found

    def locate_range(self, begin, end, column):
        """
        Returns the RIDs of all records with values in column between begin and end.
        Flush the cache first so that all new inserts are visible.
        """
        self.flush_cache()
        
        rid_list = []
        # Get all values in the range [begin, end]. Note: end + 1 to include the end.
        rid_dict = self.indices[column][begin:end + 1]
        
        if not rid_dict:
            return False
        
        for key in rid_dict:
            rid_list.append(rid_dict[key].decode('utf-8'))
        
        return rid_list if rid_list else False

    def create_index(self, column_number):
        """
        Optional: Create an index on a specific column.
        """
        # Ensure column number is valid
        if column_number < 0 or column_number >= len(self.indices):
            return False

        # Ensure the index directory exists
        index_dir = "indexes"
        os.makedirs(index_dir, exist_ok=True)

        # Define the index file path
        index_file = os.path.join(index_dir, f"{self.table_name}_index_{column_number}.txt")

        # If a previous index file exists, delete it.
        if os.path.exists(index_file):
            os.remove(index_file)

        # Create a new B+Tree index for this column.
        self.indices[column_number] = BPlusTree(index_file, order=75, cache_size=10000)

    def drop_index(self, column_number):
        """
        Optional: Drop the index of a specific column.
        """
        self.indices[column_number] = None

    def add_record(self, record):
        """
        Insert a record into the indices.
        Instead of directly inserting into the B+Tree,
        we add the insertion to the cache.
        When the cache reaches 10,000 entries, flush it using batch_insert.
        """
        rid_to_add = record.rid
        columns = list(record.columns)
        for col in range(len(columns)):
            if columns[col] is None:
                continue
            encoded_rid = rid_to_add.encode('utf-8')
            # Append the new key/value pair to the insert cache for this column.
            self.insert_cache[col].append((columns[col], encoded_rid))
            # If the cache reaches 10,000 records, flush it immediately.
            if len(self.insert_cache[col]) >= 10000:
                sorted_cache = sorted(self.insert_cache[col], key=lambda tup: tup[0])
                self.indices[col].batch_insert(sorted_cache)
                self.insert_cache[col] = []

    # Optionally, you may want to flush pending caches on any non-insert query.
    # For example, if your query class calls other functions on this index, ensure they
    # call self.flush_cache() first or wrap those calls with a decorator that flushes the cache.
