import os
from table import Table

class Database():
    def __init__(self):
        """
        Initializes the database
        """
        self.tables = {}
        self.table_directory = {}
        self.db_path = os.path.join(os.getcwd(), "database") # Database directory path
        if not os.path.exists(self.db_path):
            os.makedirs(self.db_path)

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
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
            # Create table directory inside database folder
            table_path = os.path.join(self.db_path, name)
            if not os.path.exists(table_path):
                os.makedirs(table_path)
            
            # Create new table
            table = Table(name, num_columns, key_index, table_path)
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
            # Remove table directory
            table_path = os.path.join(self.db_path, name)
            if os.path.exists(table_path):
                import shutil
                shutil.rmtree(table_path)
                
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

