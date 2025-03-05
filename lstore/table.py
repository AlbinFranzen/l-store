import os
import threading
import time
import traceback
from lstore.index import Index
from lstore.config import PAGE_SIZE, PAGE_RANGE_SIZE
from lstore.bufferpool import BufferPool
from lstore.page import Page

class Record:

    def __init__(self, base_rid, indirection, rid, start_time, schema_encoding, columns, last_updated_time=None):
        self.indirection = indirection          # points to rid of previous versioned record. base record points to most recent record -> next most recent
        self.rid = rid                          # rid = 'b' + base_rid for base record, rid = 't' + tail_rid for tail record
        self.start_time = start_time
        self.schema_encoding = schema_encoding  # bit list of 0's and 1's
        self.columns = columns                  # primary key is the first column
        self.base_rid = base_rid                # base_rid of the record (necessary for merge) when the record is a base record this is the same as rid

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
        self.name = name                                    # specifies table_name
        self.key = key                                      # specifies table_name_v
        self.num_columns = num_columns
        self.path = os.path.join(db_path,"_tables", name)   # On disk file path
        self.pr_unmerged_updates = [0]                      # Unmerged updates per page range
        self.page_directory = {}                            # {rid: (path, offset)} for each record
        self.index = Index(self)                            # Index object for this table (glorifed b+ tree storing <rid,value> pairs) 
        self.bufferpool = BufferPool(self.path)             # Bufferpool object for this table
        self.tail_page_locations = []                       # {page_range_index: path_to_last_tail_page} for each page range
        self.base_page_locations = []                       # {page_range_index: path_to_last_base_page} for each page range
        self.tail_page_indices = [0]                      # Index of last tail page for each page range
        self._init_page_range_storage()                     # Initialize page range storage directories with base and tail page 0
        self.last_path = os.path.join(self.path, "pagerange_0/base/page_0") # Path to last base page on disk (for insert)
        self.current_base_rid = 0                           # Rid of last base record
        self.current_tail_rid = 0                           # Rid of last tail record

        # Merging attributes
        self.merge_count = 0
        self.merge_thread = None
        self.merge_lock = threading.Lock()
        self.write_merge_lock = threading.Lock()
        
        # TPS for each page_range
        self.page_range_tps = {0: 0}
        self.original_per_page_range = [[0]*PAGE_RANGE_SIZE]


    def _init_page_range_storage(self):
        """Creates initial page range directory and storage"""
        base_path = os.path.join(self.path, "pagerange_0/base/")
        tail_path = os.path.join(self.path, "pagerange_0/tail/")

        os.makedirs(base_path, exist_ok=True)
        os.makedirs(tail_path, exist_ok=True)

        self.latest_tail_indices = {0:0}
        for path in [base_path, tail_path]:
            page_0_path = os.path.join(path, "page_0")
            if path == base_path:
                self.base_page_locations.append(page_0_path)
            else:
                self.tail_page_locations.append(page_0_path)
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
