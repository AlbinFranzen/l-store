import bisect
import os
from lstore.bplustree.tree import BPlusTree
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
        if self.indices[column]:  # Ensure the B+ Tree exists for the column
            column_index = self.indices[column]  # Use the pre-existing B+ Tree for the column
            print(f"column index: {column_index}")
            try:       
                if column_index[value] is not None: # C.S. string of RIDs
                    print(f"located value {column_index[value]}")
                    return column_index[value].decode('utf-8')
            except (IndexError, KeyError):
                print("no matching record")
                return False  # Return False if no matching record is found



    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column):
 
        rid_list = []
        #get all values in range begin to end
        rid_dict = self.indices[column][begin:end]
        print(f"rid dictionary: {rid_dict}")
        for key in rid_dict:
            #appends rid value
            rid_list.append(rid_dict[key].decode('utf-8'))
        
        if len(rid_list) == 0:
            return False
        else:
            return rid_list


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
        pass



    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        self.indices[column_number] = None
        pass

    def add_record(self, record):
        rid_to_add = record.rid
        columns = list(record.columns)
        #can make "for col in columns:"
        for col in range(len(columns)):
            rid_str = self.locate(col, columns[col])
            #if list is empty
            if not rid_str:
                self.indices[col][columns[col]] = rid_to_add.encode('utf-8')
            #else list is not empty
            else:
                rid_str += ("," + rid_to_add)
                rid_str = rid_str.encode('utf-8')
                self.indices[col][columns[col]] = rid_str
                
                
