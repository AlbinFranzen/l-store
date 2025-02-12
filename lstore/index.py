import os

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
    
class BPlusTree:
    def __init__(self, index_file=None, order=75, cache_size=10000):
        """
        index_file: ignored in this in-memory version.
        order: maximum number of children per internal node.
        cache_size: ignored in this implementation.
        """
        self.order = order
        self.max_keys = order - 1  # Maximum number of keys per node.
        self.root = BPlusTreeNode(is_leaf=True)

    def search(self, node, key):
        """
        Recursively find the leaf node that should contain the given key.
        """
        if node.is_leaf:
            return node
        # For an internal node, find the child pointer to follow.
        for i, item in enumerate(node.keys):
            if key < item:
                return self.search(node.children[i], key)
        return self.search(node.children[-1], key)

    def __getitem__(self, key):
        """
        If key is a slice, return a dict of key-value pairs in that range.
        Otherwise, return the value for a given key.
        """
        if isinstance(key, slice):
            start_key = key.start
            stop_key = key.stop
            result = {}
            # Find the first leaf that could contain start_key.
            node = self.search(self.root, start_key)
            while node:
                for i, k in enumerate(node.keys):
                    if k < start_key:
                        continue
                    if stop_key is not None and k >= stop_key:
                        return result
                    result[k] = node.children[i]
                node = node.next
            return result
        else:
            node = self.search(self.root, key)
            for i, k in enumerate(node.keys):
                if k == key:
                    return node.children[i]
            raise KeyError(f"Key {key} not found")

    def __setitem__(self, key, value):
        """
        Insert a single key-value pair into the B+ tree.
        """
        root = self.root
        # If the root is full, split it.
        if len(root.keys) == self.max_keys:
            new_root = BPlusTreeNode(is_leaf=False)
            new_root.children.append(root)
            self.split_child(new_root, 0)
            self.root = new_root
        self.insert_non_full(self.root, key, value)

    def insert_non_full(self, node, key, value):
        if node.is_leaf:
            # Insert the key into the leaf in sorted order.
            i = 0
            while i < len(node.keys) and node.keys[i] < key:
                i += 1
            node.keys.insert(i, key)
            node.children.insert(i, value)
        else:
            # Determine the child to descend into.
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            child = node.children[i]
            if len(child.keys) == self.max_keys:
                self.split_child(node, i)
                # After split, decide which child to use.
                if key >= node.keys[i]:
                    i += 1
            self.insert_non_full(node.children[i], key, value)

    def split_child(self, parent, index):
        """
        Split the child node at parent.children[index].
        For leaf nodes, the new node gets the latter half of keys/values and is linked.
        For internal nodes, the median key is promoted.
        """
        node = parent.children[index]
        new_node = BPlusTreeNode(is_leaf=node.is_leaf)
        mid = len(node.keys) // 2

        if node.is_leaf:
            # New leaf node gets half of the keys and values.
            new_node.keys = node.keys[mid:]
            new_node.children = node.children[mid:]
            node.keys = node.keys[:mid]
            node.children = node.children[:mid]
            new_node.next = node.next
            node.next = new_node
            split_key = new_node.keys[0]
        else:
            # For internal nodes, promote the median key.
            split_key = node.keys[mid]
            new_node.keys = node.keys[mid+1:]
            new_node.children = node.children[mid+1:]
            node.keys = node.keys[:mid]
            node.children = node.children[:mid+1]

        parent.keys.insert(index, split_key)
        parent.children.insert(index + 1, new_node)

    def batch_insert(self, pairs):
        """
        Insert many key-value pairs at once.
        The list 'pairs' must be sorted in ascending order, and if the tree is nonempty,
        the first new key must be greater than the current maximum key.
        """
        if len(self) > 0:
            current_max = self.max_key()
            if pairs[0][0] <= current_max:
                raise ValueError("Keys to batch insert must be sorted and bigger than keys currently in the tree")
        for key, value in pairs:
            self.__setitem__(key, value)

    def max_key(self):
        """
        Return the maximum key in the tree by traversing to the rightmost leaf.
        """
        node = self.root
        while not node.is_leaf:
            node = node.children[-1]
        if node.keys:
            return node.keys[-1]
        return None

    def __len__(self):
        """
        Count the number of keys stored in the tree by traversing the leaf chain.
        """
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
        self.keys = []         # For leaf nodes: keys in sorted order.
        self.children = []     # For leaf nodes: associated values.
                                # For internal nodes: child pointers.
        self.next = None       # Used in leaf nodes to link to the next leaf.
