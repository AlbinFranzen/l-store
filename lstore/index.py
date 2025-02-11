import os
from bplustree.tree import BPlusTree

class Index:

    def __init__(self, table):
        self.table_name = table.name
        self.num_columns = table.num_columns
        self.indices = [None] * self.num_columns
        self.insert_cache = {col: [] for col in range(self.num_columns)}
        self.max_keys = [None] * self.num_columns
        self.insert_cache_size = 10000
        for col in range(self.num_columns):
            self.create_index(col)

        """
        Create the index for each column
        """
    def create_index(self, column_number):
        if column_number < 0 or column_number >= self.num_columns:
            return False
        index_dir = "indexes"
        os.makedirs(index_dir, exist_ok=True)

        # Index file path
        index_file = os.path.join(index_dir, f"{self.table_name}_index_{column_number}.txt")

        # If a previous index file exists, delete it
        if os.path.exists(index_file):
            os.remove(index_file)

        # Create the new B+Tree
        self.indices[column_number] = BPlusTree(index_file, order=75, cache_size=10000)

        """
        Flush the caches for all columns. Sort and batch_insert if possible,
        otherwise fallback to individual inserts.
        """
    def flush_cache(self):
        for col, cache in self.insert_cache.items():
            if not cache:
                continue

            # Make sure batch insert is possible
            sorted_cache = sorted(cache, key=lambda tup: tup[0])
            smallest_new_key = sorted_cache[0][0]
            can_batch_insert = True
            if self.max_keys[col] is not None:
                if smallest_new_key <= self.max_keys[col]:
                    can_batch_insert = False

            # Attempt batch insert, else do individual inserts
            if can_batch_insert:
                try:
                    self.indices[col].batch_insert(sorted_cache)
                    largest_new_key = sorted_cache[-1][0]
                    self.max_keys[col] = largest_new_key
                except ValueError:
                    for (k, v) in sorted_cache:
                        self.indices[col][k] = v
                    if self.max_keys[col] is None or sorted_cache[-1][0] > self.max_keys[col]:
                        self.max_keys[col] = sorted_cache[-1][0]
            else:
                for (k, v) in sorted_cache:
                    self.indices[col][k] = v
                if self.max_keys[col] is None or sorted_cache[-1][0] > self.max_keys[col]:
                    self.max_keys[col] = sorted_cache[-1][0]

            # Clear the cache after flushing
            self.insert_cache[col] = []

        """
        Insert a record into the index caches for each column.
        Flush the cache automatically if it reaches 10,000 entries.
        """
    def add_record(self, record):
        rid_to_add = record.rid
        columns = list(record.columns)

        for col, key in enumerate(columns):
            if key is None:
                continue
            encoded_rid = rid_to_add.encode('utf-8')
            self.insert_cache[col].append((key, encoded_rid))

            # If cache hits 10,000, flush immediately for this column
            if len(self.insert_cache[col]) >= self.insert_cache_size:
                self._flush_cache_for_column(col)

        """
        Flush the cache for a single column, using the same logic as flush_cache.
        """
    def _flush_cache_for_column(self, col):
        cache = self.insert_cache[col]
        if not cache:
            return
        
        sorted_cache = sorted(cache, key=lambda tup: tup[0])
        smallest_new_key = sorted_cache[0][0]

        can_batch_insert = True
        if self.max_keys[col] is not None:
            if smallest_new_key <= self.max_keys[col]:
                can_batch_insert = False

        if can_batch_insert:
            try:
                self.indices[col].batch_insert(sorted_cache)
                self.max_keys[col] = sorted_cache[-1][0]
            except ValueError:
                for (k, v) in sorted_cache:
                    self.indices[col][k] = v
                if self.max_keys[col] is None or sorted_cache[-1][0] > self.max_keys[col]:
                    self.max_keys[col] = sorted_cache[-1][0]
        else:
            for (k, v) in sorted_cache:
                self.indices[col][k] = v
            if self.max_keys[col] is None or sorted_cache[-1][0] > self.max_keys[col]:
                self.max_keys[col] = sorted_cache[-1][0]

        self.insert_cache[col] = []

        """
        Locate function to find value
        """
    def locate(self, column, value):
        self.flush_cache()
        if value is None:
            return False
        try:
            if self.indices[column][value] is not None:
                return self.indices[column][value].decode('utf-8')
        except KeyError:
            return False
        return False

    """
    Locate range to find range
    """
    def locate_range(self, begin, end, column):
        self.flush_cache()
        results = []
        rid_dict = self.indices[column][begin:end + 1]
        if not rid_dict:
            return False
        for key in rid_dict:
            results.append(rid_dict[key].decode('utf-8'))
        return results if results else False
