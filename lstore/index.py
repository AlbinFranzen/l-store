import bisect
from bplustree import BPlusTree

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        pass



    """
    # returns the location of all records with the given value on column "column"
    # Assumes self.indices[column] is a B+ Tree.
    # Binary searches order B+ Tree list for first occurance and linearly iterates 
      until the column values for the record do not match the desired value
    """
    def locate(self, column, value):
        if self.indices[column] is not None:  # Ensure the B+ Tree exists for the column
            rids = []  # List to hold matching record IDs
            column_index = self.indices[column]  # Use the pre-existing B+ Tree for the column

            # Assuming column_index is a sorted dictionary or list of (rid, column_value)
            sorted_items = list(column_index.items())  # Convert to a sorted list of key-value pairs
            sorted_values = [column_value for rid, column_value in sorted_items]  # Extract sorted column values

            # Use bisect to find the first occurrence of the value
            index = bisect.bisect_left(sorted_values, value)

            # If the value is found, collect all matching rids
            while index < len(sorted_items) and sorted_items[index][1] == value:
                rids.append(sorted_items[index][0])
                index += 1

            # If rids contains matching record IDs, return them
            if rids:
                return rids
        return None  # Return None if no matching record is found



    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column):
        pass



    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        # Create a new B+ tree index for the given column number
        column_index = BPlusTree(f"index_{column_number}.txt", order=16)

        if column_number < 0 or column_number >= len(self.indices):
            print(f"Error: Column {column_number} is out of range. Cannot create an index.")
            return

        # Check if an index for this column already exists
        if column_number not in self.indices:
            self.indices[column_number] = column_index
            print(f"Index created for column {column_number}.")
        else:
            print(f"Index for column {column_number} already exists.")



    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        pass
