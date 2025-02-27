import os
import threading
import time

from lstore.index import Index
from  lstore.config import PAGE_SIZE, PAGE_RANGE_SIZE
# from lstore.page_range import PageRange
from lstore.bufferpool import BufferPool
from lstore.page import Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, base_rid, indirection, rid, time_stamp, schema_encoding, columns):
        self.base_rid = base_rid
        self.indirection = indirection
        self.rid = rid
        self.time_stamp = time_stamp
        self.schema_encoding = schema_encoding
        self.columns = columns

    def __repr__(self):
        return f"indirection: {self.indirection}  |  rid: {self.rid}  |  time_stamp: {self.time_stamp}  |  schema_encoding: {self.schema_encoding}  |  columns: {self.columns}\n"

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
        self.archival_path = os.path.join(db_path, "_archives", name + "_archived")
        self.pr_unmerged_updates = [0]  # Unmerged updates per page range
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
        base_path = os.path.join(self.path, "pagerange_0/base/")
        tail_path = os.path.join(self.path, "pagerange_0/tail/")

        os.makedirs(base_path, exist_ok=True)
        os.makedirs(tail_path, exist_ok=True)

        self.latest_tail_indices = {0:0}
        for path in [base_path, tail_path]:
            with open(os.path.join(path, "page_0"), 'wb') as f:
                f.write(Page().serialize())

    def __repr__(self):
        return f"Name: {self.name}\nKey: {self.key}\nNum columns: {self.num_columns}\nPage_directory: {self.page_directory}\nindex: {self.index}"
        
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


    def _merge(self, page_range_index):
        with self.merge_lock:
            # First, flush buffer pool for any pages we'll archive
            base_dir = os.path.join(self.path, f"pagerange_{page_range_index}", "base")
            tail_dir = os.path.join(self.path, f"pagerange_{page_range_index}", "tail")
            
            # Flush base pages to disk before archiving
            for base_file in sorted(os.listdir(base_dir)):
                if base_file.startswith("page_"):
                    base_path = os.path.join(base_dir, base_file)
                    base_page = self.bufferpool.get_page(base_path)
                    if base_page:
                        # Flush page to disk before archiving
                        with open(base_path, 'wb') as f:
                            f.write(base_page.serialize())
            
            # Flush tail pages to disk before archiving
            for tail_file in sorted(os.listdir(tail_dir)):
                if tail_file.startswith("page_"):
                    tail_path = os.path.join(tail_dir, tail_file)
                    tail_page = self.bufferpool.get_page(tail_path)
                    if tail_page:
                        # Flush page to disk before archiving
                        with open(tail_path, 'wb') as f:
                            f.write(tail_page.serialize())
                            
            # Archive the current table state before merging.
            self.archive_table()

            start_time = time.time()
            print("Merging records...\n")
            print(f"Merge count: {self.merge_count}")

            # Read all base and tail records into memory BEFORE removing any files or frames
            all_base_records = {}  # Map of base_rid -> base_record
            all_tail_records = []  # List of all tail records
            
            # Read all base pages and store records in memory
            base_pages = []
            base_file_paths = []
            for base_file in sorted(os.listdir(base_dir)):
                if base_file.startswith("page_"):
                    base_path = os.path.join(base_dir, base_file)
                    base_file_paths.append(base_path)
                    base_page = self.bufferpool.get_page(base_path)
                    if base_page:
                        base_pages.append(base_page)
                        # Store all records in memory
                        for base_record in base_page.read_all():
                            all_base_records[base_record.base_rid] = base_record
            
            # Read all tail pages and store records in memory
            tail_pages = []
            tail_file_paths = []
            for tail_file in sorted(os.listdir(tail_dir)):
                if tail_file.startswith("page_"):
                    tail_path = os.path.join(tail_dir, tail_file)
                    tail_file_paths.append(tail_path)
                    tail_page = self.bufferpool.get_page(tail_path)
                    if tail_page:
                        tail_pages.append(tail_page)
                        # Collect all tail records
                        all_tail_records.extend(tail_page.read_all())
            
            # Now that we have all records in memory, we can remove the frames and files
            for base_path in base_file_paths:
                self.bufferpool.abs_remove_frame(base_path)
                os.remove(base_path)
                
            for tail_path in tail_file_paths:
                self.bufferpool.abs_remove_frame(tail_path)
                os.remove(tail_path)
            
            # Merge records from base and tail pages
            # Base records are already in all_base_records dictionary
            
            #####################    IMPORTANT MERGING   #####################################
            # combine tail records into new base records
            old_page_directory = self.page_directory.copy()
            self.page_directory = {}

            updated_records = set()
            for tail_record in all_tail_records:
                if tail_record.base_rid in all_base_records:
                    base_record = all_base_records[tail_record.base_rid]
                    # Apply updates from tail records to base columns
                    updated_records.add(tail_record.base_rid)
                    # update schema and lineage too
                    base_record.columns = tail_record.columns
                    base_record.schema_encoding = [0]*len(tail_record.columns)
                    base_record.indirection = tail_record.base_rid
                    base_record.time_stamp = tail_record.time_stamp
            
            ###########################################################
            
            # Write merged records back to base pages
            print(f"Number of Merged Records: {len(all_base_records)}\n")
            
            page_index = 0
            current_page = Page()
            # We need to store the mapping of record rid to new location
            record_locations = {}  # rid -> [path, offset]
            for base_rid, base_record in all_base_records.items():
                # If current page is full, write it to disk and create new page
                if not current_page.has_capacity():
                    base_path = os.path.join(base_dir, f"page_{page_index}")
                    with open(base_path, 'wb') as f:
                        f.write(current_page.serialize())

                    page_index += 1
                    current_page = Page()
            
                # Write record to current page
                offset = current_page.write(base_record)
                base_path = os.path.join(base_dir, f"page_{page_index}")
                record_locations[base_record.base_rid] = [base_path, offset]
                self.page_directory[base_record.base_rid] = [[base_path, offset]]
        
            # Write the last page if it contains any records
            if current_page.num_records > 0:
                base_path = os.path.join(base_dir, f"page_{page_index}")
                with open(base_path, 'wb') as f:
                    f.write(current_page.serialize())
            
            # Create initial tail page_0
            tail_page_0_path = os.path.join(tail_dir, "page_0")
            with open(tail_page_0_path, 'wb') as f:
                f.write(Page().serialize())
            
            # Create a new page directory that includes archived paths
            new_page_directory = {}
            for base_rid, locations in old_page_directory.items():
                # Get current base location
                if base_rid in self.page_directory:
                    current_base_location = self.page_directory[base_rid][0]
                    new_locations = []  # Start with current base location
                    
                    # Add archived base location
                    original_base_path, base_offset = locations[0]
                    relative_base_path = os.path.relpath(original_base_path, self.path)
                    archive_base_path = os.path.join(self.archival_path, f"merge_{self.merge_count}", relative_base_path)
                    new_locations.append([archive_base_path, base_offset])
                    
                    # Add archived tail locations
                    for tail_location in locations[1:]:
                        original_tail_path, tail_offset = tail_location
                        relative_tail_path = os.path.relpath(original_tail_path, self.path)
                        archive_tail_path = os.path.join(self.archival_path, f"merge_{self.merge_count}", relative_tail_path)
                        new_locations.append([archive_tail_path, tail_offset])
                        
                        # Ensure archive directory exists
                        os.makedirs(os.path.dirname(archive_tail_path), exist_ok=True)
                    
                    new_locations.append(current_base_location)
                    new_page_directory[base_rid] = new_locations
            
            # Replace page directory with new one containing archive paths
            self.page_directory = new_page_directory
            
            # Update index
            self.index = Index(self)
            for rid, base_record in all_base_records.items():
                self.index.add_record(base_record)
                
            self.index.flush_cache()
            
            merge_duration = time.time() - start_time
            print(f"Merging completed in {merge_duration:.8f} seconds.")
            self.pr_unmerged_updates[page_range_index] = 0

    def merge(self, pagerange_index):
        # Start the merge process in a new thread
        with self.merge_lock:
            if self.merge_thread is None or not self.merge_thread.is_alive():
                self.merge_thread = threading.Thread(target=self._merge, args=(pagerange_index,))
                self.unmerged_updates = 0
                self.merge_thread.start()
            else:
                return False

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
