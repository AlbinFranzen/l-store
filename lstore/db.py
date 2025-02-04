from lstore.table import Table

class Database():

    def __init__(self):
        """
        Initializes the database
        """
        self.tables = {}          # A dictionary of tables objects
        self.table_directory = {} # A dictionary of table metadata
        pass

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
            print(f"Table '{name}' already exists, creation failed")
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
            print(f"Table '{name}' created")
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
            print(f"Table '{name}' dropped")
        else:
            print(f"Table '{name}' does not exist")
        

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        # Check if table name exists in tables dictionary
        if name in self.tables:
            # Return table
            return self.tables[name]
        else:
            print(f"Table '{name}' does not exist")
            return None
        
