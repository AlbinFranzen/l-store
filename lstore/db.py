import os
import pickle
import threading
from lstore.table import Table
from lstore.bufferpool import BufferPool

class Database():

    def __init__(self):
        """
        Initializes the database
        """
        self.tables = {}          # A dictionary of tables objects
        self.table_directory = {} # A dictionary of table metadata
        self.db_path = None
        pass

    def open(self, path):
        """
        Opens database at specified path.
        Creates directory structure if it doesn't exist.
        Loads tables from pickle files and always reinitializes their
        buffer pool and index.
        """
        self.db_path = path
        # Make paths for table storage and archives
        tables_path = os.path.join(path, "_tables")  # Store tables in a subdirectory
        self.archive_path = os.path.join(path, "_archives")
        os.makedirs(path, exist_ok=True)
        os.makedirs(tables_path, exist_ok=True)
        os.makedirs(self.archive_path, exist_ok=True)

        # Load database metadata
        db_metadata_path = os.path.join(path, "db_metadata.pickle")
        if os.path.exists(db_metadata_path):
            with open(db_metadata_path, 'rb') as f:
                try:
                    metadata = pickle.load(f)
                    self.table_directory = metadata.get('table_directory', {})
                except Exception as e:
                    print(f"Error loading database metadata: {e}")
                    self.table_directory = {}

        
        # Clear existing tables and scan directory for tables
        self.tables = {}
        
        # Look in both the main directory and the _tables subdirectory
        self._scan_directory_for_tables(path)
        if os.path.exists(tables_path):
            self._scan_directory_for_tables(tables_path)

    def _scan_directory_for_tables(self, directory_path):
        """Scan a directory for table folders and load them"""
        table_dirs = [d for d in os.listdir(directory_path) 
                    if os.path.isdir(os.path.join(directory_path, d)) 
                    and d not in ["_archives", "archive", "_tables"]]
        
        from lstore.index import Index
        
        for table_name in table_dirs:
            # Skip loading if already loaded
            if table_name in self.tables:
                continue
                
            table_path = os.path.join(directory_path, table_name)
            table_pickle_path = os.path.join(table_path, f"{table_name}.pickle")
            table_index_path = os.path.join(table_path, f"{table_name}_index.pickle")
            
            if os.path.exists(table_pickle_path):
                try:
                    with open(table_pickle_path, 'rb') as f:
                        table = pickle.load(f)
                        if table is None:
                            print(f"Error: Loaded table {table_name} is None, creating new table")
                            info = self.table_directory.get(table_name, {"num_columns": 5, "key_index": 0})
                            table = Table(table_name, info["num_columns"], info["key_index"], self.db_path)
                        
                        # Always reinitialize buffer pool
                        table.bufferpool = BufferPool(table.path)
                        
                        # Re-initialize thread locks
                        table.merge_lock = threading.Lock()
                        table.write_merge_lock = threading.Lock()
                        table.merge_thread = None
                        
                        # Initialize a new index first
                        table.index = Index(table)
                        
                        # Then try to load saved index data
                        if os.path.exists(table_index_path):
                            try:
                                with open(table_index_path, 'rb') as idx_file:
                                    index_data = pickle.load(idx_file)
                                    # Restore sorted records for primary key lookups
                                    table.index.sorted_records = index_data.get('sorted_records', [])
                                    table.index.primary_key_cache = index_data.get('primary_key_cache', {})
                                    # Restore the B+ Tree indices if keys exist
                                    for col, keys_values in index_data.get('indices', {}).items():
                                        for key, value in keys_values:
                                            if isinstance(value, bytes):
                                                table.index.indices[col][key] = value
                                            else:
                                                table.index.indices[col][key] = str(value).encode('utf-8')
                                print(f"Restored index for table {table_name}")
                            except Exception as e:
                                print(f"Error loading index for table {table_name}: {e}")
                                # Index will remain as newly initialized
                        
                        # Now reconstruct index from page directory if needed
                        self._rebuild_index_from_pages(table)
                        
                        self.tables[table_name] = table
                        
                        if table_name not in self.table_directory:
                            self.table_directory[table_name] = {
                                "name": table_name,
                                "num_columns": table.num_columns,
                                "key_index": table.key
                            }
                except Exception as e:
                    print(f"Error loading table {table_name}: {e}")
                    # Create a new table as a fallback if we have info about it
                    if table_name in self.table_directory:
                        info = self.table_directory[table_name]
                        table = Table(table_name, info["num_columns"], info["key_index"], self.db_path)
                        self.tables[table_name] = table
                        print(f"Created new table {table_name} as fallback")
    
    def _rebuild_index_from_pages(self, table):
        """Rebuild index from page files if needed"""
        # Only rebuild if index seems empty
        if not table.index.sorted_records and table.page_directory:
            print(f"Rebuilding index for table {table.name} from page directory...")
            
            # Check page directory for validity
            invalid_records = []
            for rid, locations in table.page_directory.items():
                if not locations:
                    invalid_records.append(rid)
                    continue
                    
                base_path, base_offset = locations[0]
                
                # Verify the record exists
                base_page = table.bufferpool.get_page(base_path)
                if base_page is None or base_offset >= base_page.num_records:
                    invalid_records.append(rid)
                    continue
            
            # Remove invalid entries from page directory
            for rid in invalid_records:
                if rid in table.page_directory:
                    print(f"Removing invalid page directory entry for RID {rid}")
                    del table.page_directory[rid]
            
            # Now rebuild the index with valid entries only
            for rid, locations in table.page_directory.items():
                if not locations:
                    continue
                    
                base_path, base_offset = locations[0]
                
                try:
                    # Load the base record from disk
                    base_page = table.bufferpool.get_page(base_path)
                    if base_page is None or base_offset >= base_page.num_records:
                        continue
                        
                    record = base_page.read_index(base_offset)
                    if record is None:
                        continue
                        
                    # Add record to index
                    table.index.add_record(record)
                except Exception as e:
                    print(f"Error rebuilding index for record {rid}: {e}")
            
            # Make sure to flush the cache to B+ trees
            table.index.flush_cache()
            print(f"Index rebuild complete with {len(table.index.sorted_records)} entries")

    def close(self):
        """
        Closes database and saves all tables to disk
        Runs merge on each table and each page range before closing
        """
        if not self.db_path:
            return
        
        # Run final merges on all tables and all page ranges
        for table_name, table in self.tables.items():
            print(f"Running final merges for table {table_name} before closing...")
            
            # Use pr_unmerged_updates to determine page ranges if available
            if hasattr(table, 'pr_unmerged_updates') and isinstance(table.pr_unmerged_updates, list):
                page_range_count = len(table.pr_unmerged_updates)
                print(f"Found {page_range_count} page ranges to merge for table {table_name} from metadata")
                
                # Merge each page range individually
                for pr_index in range(page_range_count):
                    try:
                        # Only merge page ranges with unmerged updates
                        if table.pr_unmerged_updates[pr_index] > 0:
                            print(f"Merging page range {pr_index} with {table.pr_unmerged_updates[pr_index]} unmerged updates...")
    
                            table.merge(pr_index)
                            # Reset unmerged updates after merging
                            table.pr_unmerged_updates[pr_index] = 0
                    except Exception as e:
                        print(f"Error merging page range {pr_index} for table {table_name}: {e}")
            else:
                # Fall back to scanning directory if pr_unmerged_updates not available
                try:
                    page_ranges = []
                    if os.path.exists(table.path):
                        page_ranges = [int(d.split('_')[1]) for d in os.listdir(table.path) 
                                      if d.startswith('pagerange_') and os.path.isdir(os.path.join(table.path, d))]
                        
                    if page_ranges:
                        print(f"Found {len(page_ranges)} page ranges by scanning directory for table {table_name}")
                        for pr_index in sorted(page_ranges):
                            print(f"Merging page range {pr_index} for table {table_name}...")
                            table.merge(pr_index)
                  
                        # If no page ranges found but _merge exists, try a general merge
                        print(f"Running general merge for table {table_name}")
                        table.merge()
                except Exception as e:
                    print(f"Error during fallback merge for table {table_name}: {e}")
            
            # Wait for any ongoing background merge threads
            if hasattr(table, 'merge_thread') and table.merge_thread and table.merge_thread.is_alive():
                print(f"Waiting for active merge thread to complete on table {table_name}...")
                try:
                    table.merge_thread.join(timeout=120)  # Wait up to 2 minutes
                    if table.merge_thread.is_alive():
                        print(f"Warning: Merge thread did not complete in time for table {table_name}")
                except Exception as e:
                    print(f"Error waiting for merge thread: {e}")
        
        # Save database metadata
        db_metadata_path = os.path.join(self.db_path, "db_metadata.pickle")
        with open(db_metadata_path, 'wb') as f:
            pickle.dump({
                'table_directory': self.table_directory
            }, f)
        
        # Save each table
        for table_name, table in self.tables.items():
            
            # Save the index first
            index_path = os.path.join(table.path, f"{table_name}_index.pickle")
            try:
                # Prepare index data - extracting what we need in a serializable format
                index_data = {
                    'sorted_records': table.index.sorted_records,
                    'primary_key_cache': table.index.primary_key_cache,
                    'indices': {}
                }
                
                # Extract key-value pairs from B+ trees
                for col in range(table.num_columns):
                    if table.index.indices[col]:
                        # Get all key-value pairs from the B+ tree
                        keys = []
                        try:
                            # Start with the minimum key
                            node = table.index.indices[col].root
                            while not node.is_leaf:
                                node = node.children[0]
                            
                            # Traverse all leaf nodes
                            while node:
                                for k, v in zip(node.keys, node.children):
                                    keys.append((k, v))
                                node = node.next
                        except Exception as e:
                            print(f"Error extracting keys for column {col}: {e}")
                            
                        index_data['indices'][col] = keys
                        
                # Save the index data
                with open(index_path, 'wb') as f:
                    pickle.dump(index_data, f)
                print(f"Saved index for table {table_name}")
            except Exception as e:
                print(f"Error saving index for {table_name}: {e}")
            
            # Flush buffer pool
            if table.bufferpool:
                for page_path, frame in table.bufferpool.frames.items():
                    if frame.dirty_bit:
                        table.bufferpool.write_to_disk(page_path, frame.page)
                
                # Save buffer pool state temporarily
                buffer_frames = table.bufferpool.frames
                # Clear buffer pool for pickling
                table.bufferpool = None
                
                # Temporarily remove thread-related attributes that can't be pickled
                merge_thread = None
                merge_lock = None
                write_merge_lock = None
                
                if hasattr(table, 'merge_thread'):
                    merge_thread = table.merge_thread
                    table.merge_thread = None
                    
                if hasattr(table, 'merge_lock'):
                    merge_lock = table.merge_lock
                    table.merge_lock = None
                    
                if hasattr(table, 'write_merge_lock'):
                    write_merge_lock = table.write_merge_lock
                    table.write_merge_lock = None
                
                try:
                    # Save table to pickle file
                    table_pickle_path = os.path.join(table.path, f"{table_name}.pickle")
                    with open(table_pickle_path, 'wb') as f:
                        pickle.dump(table, f)
                finally:
                    # Restore thread attributes
                    if merge_thread is not None:
                        table.merge_thread = merge_thread
                    if merge_lock is not None:
                        table.merge_lock = threading.Lock()
                    if write_merge_lock is not None:
                        table.write_merge_lock = threading.Lock()
                    
                # Restore buffer pool
                table.bufferpool = BufferPool(table.path)
        print("Database closed successfully")

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        # Check if table name already exists in tables dictionary
        if name in self.tables:
            # print(f"Table '{name}' already exists, creation failed")
            return None
        else:
            # Create new table
            table = Table(name, num_columns, key_index, self.db_path)
            # Add table to tables dictionary
            self.tables[name] = table
            # Add table to table_directory dictionary
            self.table_directory[name] = {
                "name": name,
                "num_columns": num_columns,
                "key_index": key_index
            }
            # print(f"Table '{name}' created")
            return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        # Check if table name exists in tables dictionary
        if name in self.tables:
        # Remove table from tables dictionary
            del self.tables[name]
        # Remove table from table_directory dictionary
            del self.table_directory[name]
            # print(f"Table '{name}' dropped")
        else:
            pass
            # print(f"Table '{name}' does not exist")
        

    
    """
    # Returns table with the passed name
    Creates the table if it exists in table_directory but not in memory
    """
    def get_table(self, name):
        # Check if table is already loaded
        if name in self.tables:
            return self.tables[name]
            
        # If not loaded but in directory, try to create it
        if name in self.table_directory:
            try:
                info = self.table_directory[name]
                table = Table(name, info["num_columns"], info["key_index"], self.db_path)
                self.tables[name] = table
                print(f"Created table {name} from directory info")
                return table
            except Exception as e:
                print(f"Error creating table {name}: {e}")
                
        # Table doesn't exist
        print(f"Table '{name}' does not exist")
        return None

