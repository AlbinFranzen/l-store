import os
import bisect

class Index:
    def __init__(self, table):
        self.table_name = table.name
        self.num_columns = table.num_columns
        self.indices = [None] * self.num_columns
        # We decided to use a cache to batch insert keys into the index for better performance
        self.insert_cache = {col: [] for col in range(self.num_columns)}
        self.max_keys = [None] * self.num_columns
        self.insert_cache_size = 10000
        for col in range(self.num_columns):
            self.create_index(col)

    """
    Create the index
    """
    def create_index(self, column_number):
        if column_number < 0 or column_number >= self.num_columns:
            return False
        index_dir = "indexes"
        os.makedirs(index_dir, exist_ok=True)
        index_file = os.path.join(index_dir, f"{self.table_name}_index_{column_number}.txt")
        if os.path.exists(index_file):
            os.remove(index_file)
        self.indices[column_number] = BPlusTree(index_file, order=75, cache_size=10000)

    """
    Flush the cache to the index
    """
    def flush_cache(self):
        for col, cache in self.insert_cache.items():
            if not cache:
                continue
            # cache is already sorted.
            try:
                self.indices[col].batch_insert(cache)
                self.max_keys[col] = cache[-1][0]
            except ValueError:
                for (k, v) in cache:
                    self.indices[col][k] = v
                if self.max_keys[col] is None or cache[-1][0] > self.max_keys[col]:
                    self.max_keys[col] = cache[-1][0]
            self.insert_cache[col] = []

    """
    Add a record to the index
    """
    def add_record(self, record):
        rid_to_add = record.rid
        for col, key in enumerate(record.columns):
            if key is None:
                continue
            encoded_rid = rid_to_add.encode('utf-8')
            # Insert maintaining sorted order with bisect
            cache = self.insert_cache[col]
            bisect.insort(cache, (key, encoded_rid))
            if len(cache) >= self.insert_cache_size:
                self._flush_cache_for_column(col)

    """
    Flush the cache for a column
    """
    def _flush_cache_for_column(self, col):
        cache = self.insert_cache[col]
        if not cache:
            return
        try:
            self.indices[col].batch_insert(cache)
            self.max_keys[col] = cache[-1][0]
        except ValueError:
            for (k, v) in cache:
                self.indices[col][k] = v
            if self.max_keys[col] is None or cache[-1][0] > self.max_keys[col]:
                self.max_keys[col] = cache[-1][0]
        self.insert_cache[col] = []

    """
    Locate a record in the index
    """
    def locate(self, column, value):
        self.flush_cache()
        if value is None:
            return False
        try:
            val = self.indices[column][value]
            if val is not None:
                return val.decode('utf-8')
        except KeyError:
            return False
        return False

    """
    Locate a range of records in the index
    """
    def locate_range(self, begin, end, column):
        self.flush_cache()
        result = {}
        rng = self.indices[column][begin: end + 1]
        if not rng:
            return False
        for key in rng:
            result[key] = self.indices[column][key].decode('utf-8')
        return result if result else False

"""
B+ Tree implementation from scratch
"""
class BPlusTree:
    def __init__(self, index_file=None, order=75, cache_size=10000):
        self.order = order
        self.max_keys = order - 1
        self.root = BPlusTreeNode(is_leaf=True)

    def search(self, key):
        # Iteratively search for the leaf node containing the key using bisect.
        node = self.root
        while not node.is_leaf:
            # Use bisect_right to find child index
            i = bisect.bisect_right(node.keys, key)
            node = node.children[i]
        return node

    def __getitem__(self, key):
        if isinstance(key, slice):
            start, stop = key.start, key.stop
            result = {}
            node = self.search(start)
            while node:
                for k, v in zip(node.keys, node.children):
                    if k < start:
                        continue
                    if stop is not None and k >= stop:
                        return result
                    result[k] = v
                node = node.next
            return result
        else:
            leaf = self.search(key)
            i = bisect.bisect_left(leaf.keys, key)
            if i < len(leaf.keys) and leaf.keys[i] == key:
                return leaf.children[i]
            raise KeyError(f"Key {key} not found")

    def __setitem__(self, key, value):
        root = self.root
        if len(root.keys) == self.max_keys:
            new_root = BPlusTreeNode(is_leaf=False)
            new_root.children.append(root)
            self.split_child(new_root, 0)
            self.root = new_root
        self.insert_non_full(self.root, key, value)

    def insert_non_full(self, node, key, value):
        if node.is_leaf:
            i = bisect.bisect_left(node.keys, key)
            node.keys.insert(i, key)
            node.children.insert(i, value)
        else:
            i = bisect.bisect_right(node.keys, key)
            child = node.children[i]
            if len(child.keys) == self.max_keys:
                self.split_child(node, i)
                if key >= node.keys[i]:
                    i += 1
            self.insert_non_full(node.children[i], key, value)

    def split_child(self, parent, index):
        node = parent.children[index]
        new_node = BPlusTreeNode(is_leaf=node.is_leaf)
        mid = len(node.keys) // 2
        if node.is_leaf:
            new_node.keys = node.keys[mid:]
            new_node.children = node.children[mid:]
            node.keys = node.keys[:mid]
            node.children = node.children[:mid]
            new_node.next = node.next
            node.next = new_node
            split_key = new_node.keys[0]
        else:
            split_key = node.keys[mid]
            new_node.keys = node.keys[mid+1:]
            new_node.children = node.children[mid+1:]
            node.keys = node.keys[:mid]
            node.children = node.children[:mid+1]
        parent.keys.insert(index, split_key)
        parent.children.insert(index + 1, new_node)

    def batch_insert(self, pairs):
        if len(self) > 0:
            current_max = self.max_key()
            if pairs[0][0] <= current_max:
                raise ValueError("Keys to batch insert must be sorted and greater than existing keys")
        for key, value in pairs:
            self.__setitem__(key, value)

    def max_key(self):
        node = self.root
        while not node.is_leaf:
            node = node.children[-1]
        return node.keys[-1] if node.keys else None

    def __len__(self):
        count = 0
        node = self.root
        while not node.is_leaf:
            node = node.children[0]
        while node:
            count += len(node.keys)
            node = node.next
        return count


class BPlusTreeNode:
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.children = []
        self.next = None
