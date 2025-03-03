import os
import threading
import time
import copy
import traceback
from lstore.index import Index
from  lstore.config import PAGE_SIZE, PAGE_RANGE_SIZE
# from lstore.page_range import PageRange
from lstore.bufferpool import BufferPool, Frame
from lstore.page import Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, base_rid, indirection, rid, start_time, schema_encoding, columns, last_updated_time=None):
        self.indirection = indirection
        self.rid = rid
        self.start_time = start_time
        self.schema_encoding = schema_encoding
        self.columns = columns
        self.base_rid = base_rid

    def __repr__(self):
        return f"indirection: {self.indirection}  |  rid: {self.rid}  |  start_time: {self.start_time}  |  schema_encoding: {self.schema_encoding}  |  columns: {self.columns}\n"

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key, db_path):
        # Table metadata
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.path = os.path.join(db_path,"_tables", name)
        self.archival_path = os.path.join(db_path, "_archives", name)
        self.pr_unmerged_updates = [0]  # Unmerged updates per page range
        self.page_directory = {}
        self.index = Index(self)
        self.bufferpool = BufferPool(self.path)
        self._init_page_range_storage()
        self.last_path = os.path.join(self.path, "pagerange_0/base/page_0")
        self.current_base_rid = 0
        self.current_tail_rid = 0
        

        # Add a record cache to minimize disk reads
        self.record_cache = {}  # {primary_key: record}
        self.max_cache_size = 10000

        # Merging attributes
        self.merge_count = 0
        self.merge_thread = None
        self.merge_lock = threading.Lock()
        self.write_merge_lock = threading.Lock()

        # Cache for tail page indices
        self.tail_page_indices = {}  # {page_range_index: last_tail_page_index}
        self.tail_page_paths = {}    # {page_range_index: path_to_last_tail_page}

        # Add a comprehensive locations cache
        self.tail_page_locations = {}  # {page_range_index: {page_index: path}}
        self.latest_tail_indices = {}  # {page_range_index: last_tail_index}
        
        # TPS for each page_range
        self.page_range_tps = {0: 0}
        
        self.original_per_page_range = [[0]*PAGE_RANGE_SIZE]

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
        base_path = os.path.join(self.path, "pagerange_0/base/")
        tail_path = os.path.join(self.path, "pagerange_0/tail/")

        os.makedirs(base_path, exist_ok=True)
        os.makedirs(tail_path, exist_ok=True)

        self.latest_tail_indices = {0:0}
        for path in [base_path, tail_path]:
            page_0_path = os.path.join(path, "page_0")
            if not os.path.exists(page_0_path):  # Only create if it doesn't exist
                with open(page_0_path, 'wb') as f:
                    f.write(Page().serialize())

    def __repr__(self):
        return f"Name: {self.name}\nKey: {self.key}\nNum columns: {self.num_columns}\nPage_directory: {self.page_directory}\nindex: {self.index}"
    
    def merge(self, pagerange_index):
        # Start the merge process in a new thread
        with self.merge_lock:
            if self.merge_thread is None or not self.merge_thread.is_alive():
                self.merge_thread = threading.Thread(target=self._merge, args=(pagerange_index,))
                self.unmerged_updates = 0
                self.merge_thread.start()
            else:
                return False

    def _merge(self, page_range_index):
        try:
            with self.merge_lock:
                start_time = time.time()
                
                # Get the last merged TPS for this page range
                last_tps = self.page_range_tps.get(page_range_index, 0)
                
                # Step 1: Process base pages without deep copying
                base_dir = os.path.join(self.path, f"pagerange_{page_range_index}", "base")
                base_pages = [os.path.join(base_dir, f) for f in sorted(os.listdir(base_dir))]
                
                # Step 2: Fetch only new tail records since last TPS
                tail_dir = os.path.join(self.path, f"pagerange_{page_range_index}", "tail")
                tail_files = sorted(
                    [f for f in os.listdir(tail_dir) if f.startswith("page_")],
                    key=lambda x: int(x.split('_')[1])
                )
                
                # Collect new tail records
                new_tail_records = []
                for tf in tail_files:
                    page_num = int(tf.split('_')[1])
                    if page_num * PAGE_SIZE < last_tps:
                        continue  # Skip already processed pages
                    tail_path = os.path.join(tail_dir, tf)
                    tail_page = self.bufferpool.get_page(tail_path)
                    for record in tail_page.read_all():
                        rid_num = int(record.rid[1:]) if record.rid.startswith('t') else 0
                        if rid_num > last_tps:
                            new_tail_records.append(record)
                
                # Sort new records by RID descending (newest first)
                new_tail_records.sort(key=lambda r: int(r.rid[1:]), reverse=True)
                
                # Step 3: Update base records incrementally
                updated_rids = set()
                for record in new_tail_records:
                    if record.base_rid in updated_rids:
                        continue  # Each base RID only once per merge
                    # Fetch base record from bufferpool
                    base_path, offset = self.page_directory.get(f"b{record.base_rid}", (None, None))
                    if not base_path:
                        continue
                    base_page = self.bufferpool.get_page(base_path)
                    base_record = base_page.read(offset)
                    if base_record:
                        # Update base record
                        base_record.columns = record.columns
                        base_record.schema_encoding = record.schema_encoding
                        base_page.write(offset, base_record)
                        updated_rids.add(record.base_rid)
                
                # Update TPS to the highest processed RID
                if new_tail_records:
                    new_tps = max(int(r.rid[1:]) for r in new_tail_records)
                    self.page_range_tps[page_range_index] = new_tps
                
                # Reset unmerged updates
                self.pr_unmerged_updates[page_range_index] = 0
                
                merge_duration = time.time() - start_time

        except Exception as e:
            print(f"Merge error: {e}")
            traceback.print_exc()

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

    def get_tail_page_location(self, pagerange_index):
        """Get the location of the current tail page for a page range"""
        
        # Ensure the page range directory exists
        tail_dir = os.path.join(self.path, f"pagerange_{pagerange_index}", "tail")
        os.makedirs(tail_dir, exist_ok=True)
        
        # Initialize with page_0 if no tail pages exist
        if not os.path.exists(os.path.join(tail_dir, "page_0")):
            empty_page = Page()
            tail_path = os.path.join(tail_dir, "page_0")
            with open(tail_path, 'wb') as f:
                f.write(empty_page.serialize())
            self.bufferpool.add_frame(tail_path, empty_page)
            return tail_path, 0
            
        # Get all tail pages and find the last one
        tail_pages = sorted([f for f in os.listdir(tail_dir) if f.startswith("page_")])
        if not tail_pages:
            return os.path.join(tail_dir, "page_0"), 0
            
        last_page_num = int(tail_pages[-1].split('_')[1])
        return os.path.join(tail_dir, f"page_{last_page_num}"), last_page_num

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
            _, last_index = self.get_tail_page_location(page_range_index)
            new_index = last_index + 1

        # Create new path

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
