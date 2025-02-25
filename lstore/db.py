from lstore.table import Table
import os
from bufferpool import BufferPool

class Database():

    def __init__(self):
        """
        Initializes the database
        """
        self.tables = {}          # A dictionary of tables objects
        self.table_directory = {} # A dictionary of table metadata
        self.db_path = None       # Path to database root folder
        self.bufferpool = None    # Bufferpool object

    # Not required for milestone1
    def open(self, path):
        # Set database path
        self.db_path = path
        # If database path does not exist, create it
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        
        # Enumerate through all tables in the database
        for table_name in os.listdir(path):
            table_dir = os.path.join(path, table_name)
            if os.path.isdir(table_dir):
                metadata_path = os.path.join(table_dir, "metadata.txt")
                if os.path.exists(metadata_path):
                    with open(metadata_path, 'r') as f:
                        lines = f.readlines()
                    # Key index is the first column, num_columns is the second column
                    if len(lines) >= 2:
                        key = int(lines[0].strip())
                        num_columns = int(lines[1].strip())
                        # Create new table
                        table = Table(table_name, num_columns, key)
                        # Add table to tables dictionary
                        self.tables[table_name] = table
                        # Add table to table_directory dictionary
                        self.table_directory[table_name] = {
                            "name": table_name,
                            "num_columns": num_columns,
                            "key_index": key
                        }
        pass

    def close(self):
        for table in self.tables.values():
            # Enumerate through all pages in the table
            for page_path, frame in table.bufferpool.frames.items():
                # Write dirty page to disk
                if frame.dirty_bit:
                    table.bufferpool.write_to_disk(page_path, frame.page)
                    # Reset dirty bit
                    frame.clear_dirty_bit()

            # Write metadata to disk
            metadata_path = os.path.join(table.path, "metadata.txt")
            with open(metadata_path, 'w') as f:
                # Key index is the first column, num_columns is the second column
                f.write(f"{table.key}\n{table.num_columns}\n")
        pass

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
            table = Table(name, num_columns, key_index)
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
    """
    def get_table(self, name):
        # Check if table name exists in tables dictionary
        if name in self.tables:
            # Return table
            # print(f"Table '{name}' got")
            return self.tables[name]
        else:
            # print(f"Table '{name}' does not exist")
            return None
        
