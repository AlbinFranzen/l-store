import os
import threading
import time
import copy

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
                print(f"Merge started with merge count: {self.merge_count}")
                
                # Capture current tail record boundary for committed records
                commit_boundary = self.current_tail_rid
                
                # Step 1: Prepare base pages and create working copies
                all_base_records = {}  # Map of base_rid -> base_record
                tail_page_references = {}  # Map of base_rid -> tail_page_location
                base_pages = []
                base_dir = os.path.join(self.path, f"pagerange_{page_range_index}", "base")
                
                # Take a snapshot of the page directory to avoid race conditions
                with self.write_merge_lock:
                    page_directory_snapshot = copy.deepcopy(self.page_directory)
                
                # Process all base pages
                for base_file in sorted(os.listdir(base_dir)):
                    base_path = os.path.join(base_dir, base_file)
                    base_page = self.bufferpool.get_page(base_path)
                    working_copy = copy.deepcopy(base_page)
                    base_pages.append(working_copy)
                    
                    # Store original base page with _original suffix
                    self.bufferpool.rename_frame(base_path, base_path + "_original")
                    
                    # Create new frame for working copy
                    new_frame = Frame(page=working_copy, page_path=base_path)
                    new_frame.set_dirty_bit()
                    self.bufferpool.frames[base_path] = new_frame
                    
                    # Process each record in base page
                    for base_record in base_page.read_all():
                        all_base_records[base_record.base_rid] = base_record
                        
                        # Only include records with valid tail pointers
                        if (isinstance(base_record.indirection, str) and 
                            base_record.indirection.startswith('t') and 
                            base_record.indirection in page_directory_snapshot):
                            tail_page_references[base_record.base_rid] = page_directory_snapshot[base_record.indirection][0]
                
                # Step 2: Find all committed tail pages and records
                # Get only tail pages that contain committed records (before commit_boundary)
                tail_dir = os.path.join(self.path, f"pagerange_{page_range_index}", "tail")
                committed_tail_paths = set()
                
                # First pass: collect all tail page paths mentioned in the directory snapshot
                for path in tail_page_references.values():
                    committed_tail_paths.add(path)
                
                # Get all committed tail records
                all_tail_records = []
                for path in sorted(list(committed_tail_paths)):
                    tail_page = self.bufferpool.get_page(path)
                    if tail_page:
                        # Only include tail records that are committed (with RID < commit_boundary)
                        for record in tail_page.read_all():
                            if (record.rid.startswith('t') and 
                                int(record.rid[1:]) < commit_boundary and
                                int(record.rid[1:]) > self.page_range_tps.get(page_range_index, 0)):
                                all_tail_records.append(record)
                
                # Sort tail records by their RID (newest first)
                all_tail_records.sort(key=lambda r: int(r.rid[1:]) if r.rid.startswith('t') else 0, reverse=True)
                
                # Step 3: Merging base and tail records
                updated_records = {rid: False for rid in all_base_records.keys()}
                num_merged = 0
                max_merged = len(all_base_records)
                
                # Process tail records newest to oldest
                for tail_record in all_tail_records:
                    if tail_record.base_rid in all_base_records and not updated_records[tail_record.base_rid]:
                        # If this base record hasn't been updated yet in this merge
                        base_record = all_base_records[tail_record.base_rid]
                        updated_records[tail_record.base_rid] = True
                        
                        # Update the base record with tail record data
                        base_record.columns = tail_record.columns
                        base_record.last_updated_time = time.time()
                        
                        # Write the updated base record
                        if base_record.rid in page_directory_snapshot:
                            path, offset = page_directory_snapshot[base_record.rid]
                            path_index = int(path.split('page_')[-1])
                            if path_index < len(base_pages):
                                base_pages[path_index].overwrite_index(offset, base_record)
                        
                        # Count merged records
                        num_merged += 1
                        if num_merged >= max_merged:
                            break
                
                # Step 4: Update the TPS (tail page sequence) for this page range
                if all_tail_records:
                    # Record the highest tail RID we've merged so we don't reprocess these again
                    self.page_range_tps[page_range_index] = max(
                        int(all_tail_records[0].rid[1:]) if all_tail_records else 0,
                        self.page_range_tps.get(page_range_index, 0)
                    )
                
                # Reset unmerged updates counter
                self.pr_unmerged_updates[page_range_index] = 0
                
                merge_duration = time.time() - start_time
                print(f"Merging completed in {merge_duration:.2f} seconds. Merged {num_merged} records.")
                self.merge_count += 1

        except Exception as e:
            print(f"Error during merge: {e}")
            import traceback
            traceback.print_exc()
            # Ensure we don't leave the counter in a bad state
            self.pr_unmerged_updates[page_range_index] = 0
            
    def archive_table(self):
        """
        Archives the entire table by copying its directory into an archive path 
        under the current merge count.
        EX: DB/table_name_archive/
        """
        #might have to make per page range instead of the entire table 
        merge_dir = os.path.join(self.archival_path, f"merge_{self.merge_count}")  # Archive destination
        os.makedirs(merge_dir, exist_ok=True)  # Ensure archive directory exists
        self.copy_directory(self.path, merge_dir)  # Copy table directory to archive
            
    def copy_directory(self, src, dst):
        """
        Recursively copies a directory without using shutil.
        Creates the destination directory structure and copies files manually.
        i.e
            DB
            ├── TABLES
            │   └── TABLES
            │       └── PAGERANGES 
            │           ├── Base
            |           |    └── 0 - P.R Size
            │           └── Tail
            |                └── 0 - P.R Size
            └── ARCHIVES
                └── TABLES
                    └── MERGE_NUM
                        └── PAGERANGES
                            ├── Base
                            │   └── 0 - P.R Size
                            └── Tail
                                └── 0 - P.R Size
        """
        os.makedirs(dst, exist_ok=True)  # Ensure the destination directory exists

        for root, _, files in os.walk(src):  # Walk through all subdirectories
            rel_path = os.path.relpath(root, src)  # Compute relative path from source
            target_dir = os.path.join(dst, rel_path)  # Corresponding path in destination

            os.makedirs(target_dir, exist_ok=True)  # Ensure each subdirectory exists in the destination

            for file in files:
                src_file = os.path.join(root, file)
                dst_file = os.path.join(target_dir, file)

                # Copy file contents manually using 4096 byte chunks (pages)
                with open(src_file, 'rb') as f_src, open(dst_file, 'wb') as f_dst:
                    while True:
                        chunk = f_src.read(PAGE_SIZE)  # Read 4096 bytes at a time
                        if not chunk:
                            break
                        f_dst.write(chunk)

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
