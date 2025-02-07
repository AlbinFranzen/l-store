import bisect
import os

from bplustree.tree import BPlusTree
"""
A data strucutre hoxlding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
class Index:

    def __init__(self, table):
        # One index for each column in the table
        self.indices = [None] *  table.num_columns
        for col in range(table.num_columns):
            self.create_index(col)



    def __repr__(self):
        return f"indices: {self.indices}"



    """
    # returns the location of all records with the given value on column "column"
    # Assumes self.indices[column] is a B+ Tree.
    # Binary searches order B+ Tree list for first occurance and linearly iterates 
      until the column values for the record do not match the desired value.
    """
    def locate(self, column, value):
        if self.indices[column] is not None:  # Ensure the B+ Tree exists for the column
            keys_index = []  # List to hold matching record IDs
            column_index = self.indices[column]  # Use the pre-existing B+ Tree for the column

            try:
                sorted_values = list(column_index.items())  # Convert generator to list
            except RuntimeError as e:
                print(f"Error: Unable to retrieve items from BPlusTree for column {column}: {e}")
                return False  # Prevent crash

            if not sorted_values:
                print(f"Warning: Index for column {column} is empty.")
                return False  # No values in index
            # Use bisect to find the first occurrence of the value
            index = bisect.bisect_left(sorted_values, value)

            # If the value is found, collect all matching rids
            while index < len(sorted_values) and sorted_values[index] == value:
                keys_index.append(index)
                index += 1

            primary_key_index = self.indices[0]  # primary key column
            primary_keys = list(primary_key_index.items())
            return_keys = []
            for index in keys_index:
                return_keys.append(primary_keys[index])

            # If primary key corresponding to desired column value exists
            if return_keys:
                return return_keys
        return False  # Return False if no matching record is found



    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column):
        rids = [] # List to hold RIDs

        #use locate and iterate through all values between begin and end
        for val in range(begin, end+1):
            result = self.locate(column, val)
            if result:
                rids.extend(result)
            
        return rids



    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        # Ensure column number is valid
        if column_number < 0 or column_number >= len(self.indices):
            print(f"Error: Column {column_number} is out of range. Cannot create an index.")
            return

        # Ensure the index directory exists
        index_dir = "indexes"
        os.makedirs(index_dir, exist_ok=True)

        # Check if an index for this column already exists
        if self.indices[column_number] is None:  # Corrected check
            self.indices[column_number] = BPlusTree(os.path.join(index_dir, f"index_{column_number}.txt"))
            print(f"Index created for column {column_number}.")
        else:
            print(f"Index for column {column_number} already exists.")



    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        self.indices[column_number] = None
        pass

    def add_record(self, record):
        pass
