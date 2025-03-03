import os
import pickle
import threading
from lstore.table import Table
from lstore.bufferpool import BufferPool
from lstore.index import Index
from lstore.query import Query

class Database:
    def __init__(self):
        self.tables = {}
        self.table_directory = {}
        self.db_path = None

    def open(self, path):
        self.db_path = path
        os.makedirs(path, exist_ok=True)
        os.makedirs(os.path.join(path, "_tables"), exist_ok=True)
        
        # Load database metadata
        meta_path = os.path.join(path, "db_metadata.pickle")
        if os.path.exists(meta_path):
            with open(meta_path, 'rb') as f:
                self.table_directory = pickle.load(f).get('table_directory', {})
        
        # Load tables
        for table_name, info in self.table_directory.items():
            # Try to load the table metadata
            metadata_path = os.path.join(path, "_tables", f"{table_name}_metadata.pickle")
            if os.path.exists(metadata_path):
                with open(metadata_path, 'rb') as f:
                    metadata = pickle.load(f)
                    table = Table(table_name, metadata['num_columns'], metadata['key'], self.db_path)
                    table.page_directory = metadata.get('page_directory', {})
                    
                    # Create query object
                    table.query = Query(table)
                    if 'current_base_rid' in metadata:
                        table.query.current_base_rid = metadata['current_base_rid']
                    if 'current_tail_rid' in metadata:
                        table.query.current_tail_rid = metadata['current_tail_rid']
                    
                    self.tables[table_name] = table
                    print(f"Loaded table: {table_name}")

    def close(self):
        # Save database metadata
        with open(os.path.join(self.db_path, "db_metadata.pickle"), 'wb') as f:
            pickle.dump({'table_directory': self.table_directory}, f)
        
        # Save each table
        for name, table in self.tables.items():
            # Save table metadata
            metadata = {
                'name': name,
                'num_columns': table.num_columns,
                'key': table.key,
                'page_directory': table.page_directory
            }
            
            # Add query info if available
            if hasattr(table, 'query') and table.query:
                metadata['current_base_rid'] = table.query.current_base_rid
                metadata['current_tail_rid'] = table.query.current_tail_rid
            
            # Save metadata
            meta_path = os.path.join(self.db_path, "_tables", f"{name}_metadata.pickle")
            os.makedirs(os.path.dirname(meta_path), exist_ok=True)
            with open(meta_path, 'wb') as f:
                pickle.dump(metadata, f)
            
            # Write buffer pages to disk
            for path, frame in table.bufferpool.frames.items():
                try:
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, 'wb') as f:
                        f.write(frame.page.serialize())
                except Exception as e:
                    print(f"Error writing {path}: {e}")
        
        print("Database closed")

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
                table = Table(name, metadata['num_columns'], metadata['key'], self)
                table.page_directory = metadata.get('page_directory', {})
                table.query = Query(table)
                if 'current_base_rid' in metadata:
                    table.query.current_base_rid = metadata['current_base_rid']
                if 'current_tail_rid' in metadata:
                    table.query.current_tail_rid = metadata['current_tail_rid']
                self.tables[name] = table
                return table
            except Exception as e:
                print(f"Error loading {name}: {e}")
        
        # Fall back to table directory or return None
        if name in self.table_directory:
            info = self.table_directory[name]
            table = Table(name, info["num_columns"], info["key_index"], self)
            self.tables[name] = table
            return table
        
        return None
