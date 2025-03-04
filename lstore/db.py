import os
import pickle
from lstore.table import Table

class Database:
    def __init__(self):
        self.tables = {}
        self.table_directory = {}
        self.db_path = None

    def open(self, path):
        """Open database at specified path"""
        self.db_path = path
        os.makedirs(path, exist_ok=True)
        os.makedirs(os.path.join(path, "_tables"), exist_ok=True)
        os.makedirs(os.path.join(path, "indexes"), exist_ok=True)
        
        # Load database metadata
        meta_path = os.path.join(path, "db_metadata.pickle")
        if os.path.exists(meta_path):
            with open(meta_path, 'rb') as f:
                self.table_directory = pickle.load(f).get('table_directory', {})
    

    def close(self):
        """Close database and save all tables and indices"""
        # Wait for any ongoing merge operations to complete
        for name, table in self.tables.items():
            if table.merge_thread and table.merge_thread.is_alive():
                try:
                    table.merge_thread.join(timeout=30)  # Wait up to 30 seconds
                    if table.merge_thread.is_alive():
                        print(f"Warning: Merge operation on table '{name}' timed out")
                except Exception as e:
                    print(f"Error waiting for merge thread: {e}")
            
        
        # Save database metadata
        with open(os.path.join(self.db_path, "db_metadata.pickle"), 'wb') as f:
            pickle.dump({'table_directory': self.table_directory}, f)
        
        # Save each table and its indices
        for name, table in self.tables.items():
            # Save table metadata
            metadata = {
                'name': name,
                'num_columns': table.num_columns,
                'key': table.key,
                'page_directory': table.page_directory,
                'current_base_rid': table.current_base_rid,
                'current_tail_rid': table.current_tail_rid
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
                
            # Write buffer pages to disk
            for frame in table.bufferpool.frames.values():
                table.bufferpool.write_to_disk(frame.page_path, frame.page)
            

    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index, self.db_path)
        self.tables[name] = table
        self.table_directory[name] = {
            "name": name, "num_columns": num_columns, "key_index": key_index
        }
        return table


    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]
            del self.table_directory[name]


    def get_table(self, name):
        # Return existing table
        if name in self.tables:
            return self.tables[name]
        
        # Try to load from metadata
        metadata_path = os.path.join(self.db_path, "_tables", f"{name}_metadata.pickle")
        if os.path.exists(metadata_path):
            try:
                with open(metadata_path, 'rb') as f:
                    metadata = pickle.load(f)
                
                # Create table and restore its state
                table = Table(name, metadata['num_columns'], metadata['key'], self.db_path)
                table.page_directory = metadata.get('page_directory', {})
                
                # Restore base/tail RIDs
                table.current_base_rid = metadata.get('current_base_rid', 0)
                table.current_tail_rid = metadata.get('current_tail_rid', 0)
                
                # Load index data
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
                print(f"Error loading {name}: {e}")
        
        # Fall back to table directory
        if name in self.table_directory:
            info = self.table_directory[name]
            table = Table(name, info["num_columns"], info["key_index"], self.db_path)
            self.tables[name] = table
            return table
        
        return None
