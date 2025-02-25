import os
import threading
import time

from lstore.index import Index
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

    def __init__(self, name, num_columns, key, database=None):
        # Table metadata
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.database = database
        self.path = os.path.join("database", name)
        self.page_directory = {}
        self.index = Index(self)
        self.bufferpool = BufferPool(self.path)
        self._init_page_range_storage()
        self.last_path = os.path.join(self.path, "pagerange_0/base/page_0")

        # Add a record cache to minimize disk reads
        self.record_cache = {}  # {primary_key: record}
        self.max_cache_size = 10000

        # Merging attributes
        self.merge_count = 0
        self.total_updates = 0
        self.unmerged_updates = 0
        self.merge_thread = None
        self.merge_lock = threading.Lock()
        self.write_merge_lock = threading.Lock()

        # Cache for tail page indices
        self.tail_page_indices = {}  # {page_range_index: last_tail_page_index}
        self.tail_page_paths = {}    # {page_range_index: path_to_last_tail_page}

        # Add a comprehensive locations cache
        self.tail_page_locations = {}  # {page_range_index: {page_index: path}}
        self.latest_tail_indices = {}  # {page_range_index: last_tail_index}

    def cache_record(self, key, record):
        """
        Store frequently accessed records in memory
        """
        # Simple LRU: remove random entry if full
        if len(self.record_cache) >= self.max_cache_size:
            self.record_cache.pop(next(iter(self.record_cache)))
        self.record_cache[key] = record

    def get_cached_record(self, key):
        """
        Get record from cache if available
        """
        return self.record_cache.get(key)

    def invalidate_cache(self, key):
        """
        Remove record from cache when updated
        """
        if key in self.record_cache:
            del self.record_cache[key]

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

    def _merge(self):
        with self.merge_lock:
            start_time = time.time()  # Start timing the merge process
            print("Merging records...\n")

            # Log the current state before merging
            print(f"Current merge count: {self.merge_count}")
            print(f"Current total updates before merge: {self.total_updates}")
            print(f"Current unmerged updates: {self.unmerged_updates}\n")

            end_time = time.time()  # End timing the merge process
            merge_duration = end_time - start_time  # Calculate duration

            # Log the results of the merge
            print(f"Merging completed in {merge_duration:.2f} seconds.")
            print(f"Total merges: {self.merge_count}")
            print(f"Total updates merged: {self.total_updates}\n")

    def merge(self):
        # Start the merge process in a new thread
        with self.merge_lock:
            if self.merge_lock:
                #archive before merge process
                if self.database:
                    self.database.save_archive(self.name)
                    
            if self.merge_thread is None or not self.merge_thread.is_alive():
                self.merge_thread = threading.Thread(target=self._merge)
                self.total_updates += self.unmerged_updates
                self.unmerged_updates = 0
                self.merge_thread.start()

    def get_tail_page_path(self, page_range_index):
        """
        Get the path to the last tail page in a page range.
        Creates caching mechanism to avoid expensive directory scans.
        """
        if page_range_index in self.tail_page_indices:
            # Use cached value
            return self.tail_page_paths[page_range_index]

        # Cache miss - scan directory once
        tail_dir = os.path.join(self.path, f"pagerange_{page_range_index}", "tail")
        try:
            tail_files = [f for f in os.listdir(tail_dir) if f.startswith("page_")]
            if (tail_files):
                last_tail_index = max((int(f.split("page_")[1]) for f in tail_files))
            else:
                last_tail_index = 0

            # Cache the result
            self.tail_page_indices[page_range_index] = last_tail_index
            path = f"database/{self.name}/pagerange_{page_range_index}/tail/page_{last_tail_index}"
            self.tail_page_paths[page_range_index] = path
            return path
        except Exception as e:
            # Handle case where directory doesn't exist yet
            print(f"Error scanning tail directory: {e}")
            return f"database/{self.name}/pagerange_{page_range_index}/tail/page_0"

    def update_tail_page_index(self, page_range_index, new_index):
        """
        Update the cached tail page index when a new tail page is created
        """
        self.tail_page_indices[page_range_index] = new_index
        self.tail_page_paths[page_range_index] = f"database/{self.name}/pagerange_{page_range_index}/tail/page_{new_index}"

    def get_tail_page_location(self, page_range_index, create_if_missing=False):
        """
        Get the latest tail page location for a page range.
        Efficiently uses cache to avoid filesystem operations.

        Args:
            page_range_index: Index of the page range
            create_if_missing: Whether to create the directory if missing

        Returns:
            tuple: (path, page_index) of the latest tail page
        """
        # Check if we have this page range in our cache
        if page_range_index in self.latest_tail_indices:
            last_index = self.latest_tail_indices[page_range_index]
            path = self.tail_page_locations[page_range_index][last_index]
            return path, last_index

        # Cache miss - scan the directory
        tail_dir = os.path.join(self.path, f"pagerange_{page_range_index}", "tail")

        # Create directory if needed
        if create_if_missing and not os.path.exists(tail_dir):
            os.makedirs(tail_dir, exist_ok=True)
            tail_path = os.path.join(tail_dir, "page_0")
            with open(tail_path, 'wb') as f:
                f.write(Page().serialize())

            # Update cache
            self.latest_tail_indices[page_range_index] = 0
            self.tail_page_locations.setdefault(page_range_index, {})[0] = tail_path
            return tail_path, 0

        # Scan existing directory
        try:
            tail_files = [f for f in os.listdir(tail_dir) if f.startswith("page_")]
            if tail_files:
                # Find highest page index
                last_tail_index = max(int(f.split("page_")[1]) for f in tail_files)
            else:
                last_tail_index = 0
                tail_path = os.path.join(tail_dir, "page_0")
                with open(tail_path, 'wb') as f:
                    f.write(Page().serialize())

            # Update cache
            self.latest_tail_indices[page_range_index] = last_tail_index
            page_paths = {}

            # Cache all tail page paths for this range
            for i in range(last_tail_index + 1):
                path = f"database/{self.name}/pagerange_{page_range_index}/tail/page_{i}"
                page_paths[i] = path

            self.tail_page_locations[page_range_index] = page_paths

            # Return the latest
            return page_paths[last_tail_index], last_tail_index

        except Exception as e:
            print(f"Error accessing tail directory: {e}")
            if create_if_missing:
                # Create directory and initial page
                os.makedirs(tail_dir, exist_ok=True)
                tail_path = os.path.join(tail_dir, "page_0")
                with open(tail_path, 'wb') as f:
                    f.write(Page().serialize())

                # Update cache
                self.latest_tail_indices[page_range_index] = 0
                self.tail_page_locations.setdefault(page_range_index, {})[0] = tail_path
                return tail_path, 0
            return None, None

    def create_new_tail_page(self, page_range_index):
        """
        Create a new tail page in the specified page range.
        Updates location cache and returns the path to the new page.

        Returns:
            tuple: (path, page_index) of the newly created tail page
        """
        # Get current tail index
        if page_range_index in self.latest_tail_indices:
            last_index = self.latest_tail_indices[page_range_index]
            new_index = last_index + 1
        else:
            # Get it from filesystem
            _, last_index = self.get_tail_page_location(page_range_index, create_if_missing=True)
            new_index = last_index + 1

        # Create new path
        tail_dir = os.path.join(self.path, f"pagerange_{page_range_index}", "tail")
        os.makedirs(tail_dir, exist_ok=True)
        new_path = os.path.join(tail_dir, f"page_{new_index}")

        # Create empty file on disk
        with open(new_path, 'wb') as f:
            f.write(Page().serialize())

        # Update cache
        self.latest_tail_indices[page_range_index] = new_index
        self.tail_page_locations.setdefault(page_range_index, {})[new_index] = new_path

        return new_path, new_index