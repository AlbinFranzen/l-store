import os
import pickle
from lstore.table import Table

class Database:
    def __init__(self):
        self.tables = {}            # In-memory cache of tables
        self.table_directory = {}   # Basic table info - "name": {"num_columns": num_columns, "key_index": key_index}
        self.db_path = None         # path to the database directory

    def open(self, path):
        """Open database at specified path, with tables and indices from disk"""
        self.db_path = path
        os.makedirs(path, exist_ok=True)
        os.makedirs(os.path.join(path, "_tables"), exist_ok=True)
        os.makedirs(os.path.join(path, "indexes"), exist_ok=True)
        
        # Load database metadata (just table directory info)
        meta_path = os.path.join(path, "db_metadata.pickle")
        if os.path.exists(meta_path):
            with open(meta_path, 'rb') as f:
                self.table_directory = pickle.load(f)
        
        # Clear any cached tables
        self.tables = {}

    def close(self):
        """Close database and save all tables metadata and indices"""
        # Wait for any ongoing merge operations to complete
        for name, table in self.tables.items():
            if hasattr(table, 'merge_thread') and table.merge_thread and table.merge_thread.is_alive():
                try:
                    table.merge_thread.join(timeout=30)
                    if table.merge_thread.is_alive():
                        print(f"Warning: Merge operation on table '{name}' timed out")
                except Exception as e:
                    print(f"Error waiting for merge thread: {e}")
        
        # Save basic table directory info
        with open(os.path.join(self.db_path, "db_metadata.pickle"), 'wb') as f:
            pickle.dump(self.table_directory, f)
        
        # Save each table's metadata and index separately
        for name, table in self.tables.items():
            # Save complete table metadata
            metadata = {
                'name': name,
                'num_columns': table.num_columns,
                'key': table.key,
                'page_directory': table.page_directory,
                'current_base_rid': table.current_base_rid,
                'current_tail_rid': table.current_tail_rid,
                'tail_page_locations': table.tail_page_locations,
                'base_page_locations': table.base_page_locations,
                'tail_page_indices': table.tail_page_indices,
                'pr_unmerged_updates': table.pr_unmerged_updates,
                'page_range_tps': table.page_range_tps,
                'last_path': table.last_path,
                'merge_count': table.merge_count
            }
            
            # Save metadata
            meta_path = os.path.join(self.db_path, "_tables", f"{name}_metadata.pickle")
            os.makedirs(os.path.dirname(meta_path), exist_ok=True)
            with open(meta_path, 'wb') as f:
                pickle.dump(metadata, f)
                
            # Save index data separately (without locks)
            index_path = os.path.join(self.db_path, "_tables", f"{name}_index.pickle")
            index_data = {
                'primary_key_cache': table.index.primary_key_cache,
                'indices': table.index.indices,
                'sorted_records': table.index.sorted_records
            }
            with open(index_path, 'wb') as f:
                pickle.dump(index_data, f)
                
            # Write any dirty buffer pages to disk
            for path, frame in table.bufferpool.frames.items():
                if frame.dirty_bit:
                    table.bufferpool.write_to_disk(path, frame.page)

    def create_table(self, name, num_columns, key_index):
        """Create table with specified name, number of columns, and key index"""
        table = Table(name, num_columns, key_index, self.db_path)
        self.tables[name] = table
        self.table_directory[name] = {
            "num_columns": num_columns, "key_index": key_index
        }
        return table

    def drop_table(self, name):
        """Remove table from memory and directory"""
        if name in self.tables:
            del self.tables[name]
            
        if name in self.table_directory:
            del self.table_directory[name]
            
        # Remove metadata and index files
        meta_path = os.path.join(self.db_path, "_tables", f"{name}_metadata.pickle")
        if os.path.exists(meta_path):
            os.remove(meta_path)
            
        index_path = os.path.join(self.db_path, "_tables", f"{name}_index.pickle")
        if os.path.exists(index_path):
            os.remove(index_path)

    def get_table(self, name):
        """Get table by name, creating a new instance from metadata"""
        # Return existing table if it's already loaded
        if name in self.tables:
            return self.tables[name]
        
        # Check if table metadata exists
        metadata_path = os.path.join(self.db_path, "_tables", f"{name}_metadata.pickle")
        if os.path.exists(metadata_path):
            try:
                # Load metadata
                with open(metadata_path, 'rb') as f:
                    metadata = pickle.load(f)
                
                # Create fresh table instance
                table = Table(name, metadata['num_columns'], metadata['key'], self.db_path)
                
                # Apply metadata to reconstruct table state
                table.page_directory = metadata['page_directory']
                table.current_base_rid = metadata['current_base_rid']
                table.current_tail_rid = metadata['current_tail_rid']
                table.tail_page_locations = metadata['tail_page_locations']
                table.base_page_locations = metadata['base_page_locations']
                table.tail_page_indices = metadata['tail_page_indices']
                table.pr_unmerged_updates = metadata['pr_unmerged_updates']
                table.page_range_tps = metadata['page_range_tps']
                table.last_path = metadata['last_path']
                table.merge_count = metadata.get('merge_count', 0)
                
                # Load index data separately
                index_path = os.path.join(self.db_path, "_tables", f"{name}_index.pickle")
                if os.path.exists(index_path):
                    with open(index_path, 'rb') as f:
                        index_data = pickle.load(f)
                        # Apply index data to the newly created index
                        table.index.primary_key_cache = index_data.get('primary_key_cache', {})
                        table.index.indices = index_data.get('indices', {})
                        table.index.sorted_records = index_data.get('sorted_records', [])
                
                # Add to tables dictionary
                self.tables[name] = table
                return table
                
            except Exception as e:
                print(f"Error loading {name} metadata: {e}")
                import traceback
                traceback.print_exc()
        
        # Fall back to table directory if metadata doesn't exist
        if name in self.table_directory:
            info = self.table_directory[name]
            table = Table(name, info["num_columns"], info["key_index"], self.db_path)
            self.tables[name] = table
            return table
        
        return None
