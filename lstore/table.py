import os
import threading
import time
import traceback
from lstore.index import Index
from lstore.config import PAGE_SIZE, PAGE_RANGE_SIZE
from lstore.bufferpool import BufferPool
from lstore.page import Page
import copy

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
        self.page_range_tps = [0]


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
                
                # Get the last merged TPS for this page range
                last_tps = self.page_range_tps[page_range_index]    
                
                # Get all the base records
                base_dir = os.path.join(self.path, f"pagerange_{page_range_index}", "base/page_")
                base_paths = [base_dir + str(f) for f in range(len(self.base_page_locations))]
                
                tail_paths = set()
                # Loop through base pages to get tail page paths
                for path in base_paths:
                    base_page = self.bufferpool.get_page(path)
                    base_records = base_page.read_all()
                    for base_record in base_records:
                        tail_rid = base_record.indirection
                        if base_record.indirection == base_record.rid: # if base record has not been updated
                            continue
                        tail_path, _ = self.page_directory[tail_rid]
                        tail_paths.add(tail_path)
                    self.bufferpool.unpin_page(path)
                
                tail_paths = list(tail_paths)
                # Sort tail paths by page number
                tail_paths.sort(key=lambda x: int(x.split('_')[-1]))
                
                updated_rids = set()
                first_update = False
                reached_last_tps = False
                last_tps_temp = None
                #traverse the tail pages in reverse to add most recent updates to the base page
                for tail_path in reversed(tail_paths):
                    if reached_last_tps: # if we have reached the last tps, stop
                        break
                    new_tail_records = self.bufferpool.get_page(tail_path).read_all()
                    self.bufferpool.unpin_page(tail_path)
                    if first_update == False: # set new tps to the last rid in the tail page
                        last_tps_temp = int(new_tail_records[-1].rid[1:])
                        first_update = True
                    for record in reversed(new_tail_records):
                        if int(record.rid[1:]) <= last_tps:
                            reached_last_tps = True
                            break
                        #if you find previous update skip
                        if record.base_rid in updated_rids:
                            continue  # Each base RID only once per merge
                        # Fetch base record from bufferpool
                        base_path, offset = self.page_directory[record.base_rid]
                        if not base_path:
                            print("no path found")
                            continue
                        base_page = self.bufferpool.get_page(base_path)
                        base_record = base_page.read_index(offset)
                        if base_record:
                            # Update base record
                            base_record.columns = record.columns
                            updated_rids.add(record.base_rid)
                
                # Reset unmerged updates
                if last_tps_temp is not None:
                    self.page_range_tps[page_range_index] = last_tps_temp
                self.pr_unmerged_updates[page_range_index] = 0

        except Exception as e:
            print(f"Merge error: {e}")
            traceback.print_exc()
