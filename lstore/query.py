from uuid import uuid4
from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
#from lstore.page_range import PageRange
from lstore.config import *
import time
import copy
import os

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        self.current_base_rid = 0
        self.current_tail_rid = 0
        pass
    
    def __repr__(self):
        return f"Table:\n{self.table}\ncurrent_base_rid: {self.current_base_rid}\ncurrent_tail_rid: {self.current_tail_rid}"
    
    def update_base_rid(self):
        self.current_base_rid += 1
        pass
    
    def update_tail_rid(self):
        self.current_tail_rid += 1
        pass
    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):

        # get rids with the primary key
        base_rid = self.table.index.locate(0, primary_key)
        if base_rid == False:
            return False
        
        if isinstance(base_rid, bytes):
            base_rid = base_rid.decode()
        if not base_rid:
            return False

        # get all records in lineage
        lineage = self._traverse_lineage(base_rid)

        # create a new tail record
        delete_record = Record(
                        lineage[-1].rid,
                        "t" + str(self.current_tail_rid),
                        time.time(),
                        schema_encoding = [0] * len(lineage[0].schema_encoding),
                        columns = [None] * len(lineage[0].columns)
                    )
        
        # update tail record's rid
        lineage[-1].indirection = delete_record.rid

        # ensure space exists
        if not self.table.page_ranges[-1].has_capacity():
            pass
        if not self.table.page_ranges[-1].tail_pages[-1].has_capacity():
            self.table.page_ranges[-1].tail_pages.append(Page())   

        # write record to tail page
        offset = self.table.page_ranges[-1].tail_pages[-1].write(delete_record)
        tail_page_index = len(self.table.page_ranges[-1].tail_pages) - 1
        page_range_index = len(self.table.page_ranges) - 1

        # update page directory
        if lineage[0].rid in self.table.page_directory:
            self.table.page_directory[lineage[0].rid] = []
        self.table.page_directory[lineage[0].rid].append([page_range_index, tail_page_index, offset])

        # update tail rid
        self.update_tail_rid()

        return True
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    # FOR BASE PAGES
    """
    def insert(self, *columns):
        if not self._verify_insert_input(*columns): # Verify input
            return False
        record = Record(None, f"b{self.current_base_rid}", time.time(), [0] * len(columns), [*columns]) # Create record
        self.table.index.add_record(record) # Add record to index
        
         # Get last page from bufferpool
        last_path = self.table.last_path
        self.table.bufferpool.add_frame(last_path) # Add last page frame to bufferpool
        last_page = self.table.bufferpool.get_page(last_path) # Get frame with last page from bufferpool
        
        # Check if space exists
        last_pagerange_index = int(last_path.split('pagerange_')[1].split('/')[0])
        last_page_index = int(last_path.split('page_')[1].split('.')[0])
        
        if last_page.has_capacity(): # If space in last page, write to last page
            last_page.write(record)
            insert_path = self.table.last_path
            offset = last_page.num_records - 1
        else: # Else create new page
            new_page = Page()
            new_page.write(record)
            insert_path = last_path
            offset = 0
            if last_page_index < PAGE_RANGE_SIZE: # If space in page range, write to new page in page range
                insert_path = f"database/{self.table.name}/pagerange_{last_pagerange_index}/base/page_{last_page_index + 1}.csv"
            else: # If no space in page range, create new page range
                new_pagerange_path = f"database/{self.table.name}/pagerange_{last_pagerange_index + 1}"
                os.makedirs(f"{new_pagerange_path}/base")
                os.makedirs(f"{new_pagerange_path}/tail") 
                insert_path = f"{new_pagerange_path}/base/page_0.csv"
            
            # Add new page to bufferpool
            self.table.last_path = insert_path
            self.table.bufferpool.add_frame(insert_path, new_page)
                   
        # Add new location to page directory 
        self.table.page_directory[f"b{self.current_base_rid}"] = [insert_path, offset]
        self.current_base_rid += 1
        return True
    
    def _verify_insert_input(self, *columns):
        for column in columns:
            if not isinstance(column, int):
                return False
        return True
    
    

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        # Get the base rids of the records with the search key
        rid_combined_string = self.table.index.locate(search_key_index, search_key)
        if rid_combined_string == False:
            return False
        rid_list = rid_combined_string.split(",")
        
        # Merge the lineage
        records = []
        for rid in rid_list:
            new_record = self._get_merged_lineage(rid, projected_columns_index)
            records.append(new_record)
        
        return records 
    
    def _get_merged_lineage(self, base_rid, projected_columns_index):  
        lineage = self._traverse_lineage(base_rid)
        base_values = list(lineage[0].columns)
        for record in lineage:
            new_values = record.columns
            for val in range(len(new_values)):
                if new_values[val] != None:
                    base_values[val] = new_values[val]
        new_record = Record(lineage[0].rid, lineage[0].indirection, record.time_stamp, record.schema_encoding, [element for element, bit in zip(base_values, projected_columns_index) if bit == 1])
        return new_record
   
    
    # Get list of records from base_rid
    def _traverse_lineage(self, base_rid):
        lineage = []
        if isinstance(base_rid, bytes):
            base_rid = base_rid.decode()  # Ensure base_rid is a string

        if base_rid not in self.table.page_directory:
            return []

        # Get the path from page directory
        base_path, base_offset = self.table.page_directory[base_rid]
        
        # Read the base record
        base_page = self.table.bufferpool.get_page(base_path)
        base_record = base_page.read_index(base_offset)
        lineage.append(base_record)
        
        # Follow indirection chain through tail records
        current_rid = base_record.indirection
        while current_rid and current_rid != base_rid:
            if current_rid in self.table.page_directory:
                tail_path, offset = self.table.page_directory[current_rid]
                tail_page = self.table.bufferpool.get_page(tail_path)
                tail_record = tail_page.read_index(offset)
                lineage.append(tail_record)
                current_rid = tail_record.indirection
            else:
                break

        return lineage
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    # RELATIVE_VERSION USAGE: (-1, -2, etc)
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        rids_combined = self.table.index.locate(search_key_index, search_key)
        if not rids_combined:
            return False
        rid_list = rids_combined.split(",")
        
        # Here, each element in record_lineages is already a lineage (a list of records)
        results = []
        for rid in rid_list:
            lineage = self._traverse_lineage(rid)
            if relative_version - 1 < 0:
                target_index = min(len(lineage) + relative_version - 1, len(lineage)-1)
            else:
                target_index = relative_version - 1

            # Start with the base record's columns (assumed to be in record_list[0])
            merged_columns = list(lineage[0].columns)

            # Merge updates from record_list[1] through record_list[target_index] (inclusive)
            for record in lineage[1:target_index + 1]:
                for i, value in enumerate(list(record.columns)):
                    if value is not None:
                        merged_columns[i] = value
            
            
            record = Record(lineage[0].rid, lineage[0].indirection, lineage[-1].time_stamp, lineage[-1].schema_encoding, [element for element, bit in zip(merged_columns, projected_columns_index) if bit == 1])
            results.append(record)
        return results


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    # FOR TAIL PAGES
    """
    def update(self, primary_key, *columns):
        # Get the rids of the records with the primary key
        base_rid = self.table.index.locate(0, primary_key)
        if base_rid is False:
            return False

        if isinstance(base_rid, bytes):
            base_rid = base_rid.decode()  # Decode byte string to regular string
        if not base_rid:
            return False

        lineage = self._traverse_lineage(base_rid)
        if not lineage:
            return False  # or handle this appropriately

        # Now access lineage safely
        record = Record(
            lineage[0].rid,  # Base RID of lineage
            "t" + str(self.current_tail_rid),
            time.time(),
            lineage[0].schema_encoding,  # Retain schema encoding
            [*columns]  # Columns could be updated if needed
        )

        # Update old records
        lineage[0].schema_encoding = record.schema_encoding # update base record's schema encoding
        lineage[-1].indirection = record.rid   # tail record's rid
        
        # Make sure space exists
        if not self.table.page_ranges[-1].has_capacity(): # If page range is full, create new one
            #self.table.page_ranges.append(PageRange())
            pass
        if not self.table.page_ranges[-1].tail_pages[-1].has_capacity(): # If base page is full, create new one
            self.table.page_ranges[-1].tail_pages.append(Page())
            
        # Write and get location 
        offset = self.table.page_ranges[-1].tail_pages[-1].write(record) 
        tail_page_index = len(self.table.page_ranges[-1].tail_pages) - 1
        page_range_index = len(self.table.page_ranges) - 1 
        
        # Add new location to page directory 
        self.table.page_directory[lineage[0].rid].append([page_range_index, tail_page_index, offset])
        self.current_tail_rid += 1
        return True

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        # Get all RIDs in the range (inclusive)
        rid_combined_strings = self.table.index.locate_range(start_range, end_range, 0)
        if rid_combined_strings == False:
            return False
        
        # Flatten the list of RIDs and split any combined RID strings
        rids = []
        for rid_string in rid_combined_strings:
            rids.extend(rid_string.split(","))
        
        range_sum = 0
        # Get the merged record for each RID and add the specified column value
        for rid in rids:
            merged_record = self._get_merged_lineage(rid, [1] * self.table.num_columns)
            if merged_record and merged_record.columns[aggregate_column_index] is not None:
                range_sum += merged_record.columns[aggregate_column_index]
            
        return range_sum
      
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        range_sum = 0    
        rids = self.table.index.locate_range(start_range, end_range, 0)
        if rids == False:
            return False
        
        #traverse tree for each rid found in range
        for rid in rids:
            lineage = self._traverse_lineage(rid)
            if relative_version - 1 < 0:
                target_index = min(len(lineage) + relative_version - 1, len(lineage)-1)
            else:
                target_index = relative_version - 1
            
            # Start with the base record's columns (assumed to be in record_list[0])
            merged_columns = list(lineage[0].columns)

            # Merge updates from record_list[1] through record_list[target_index] (inclusive)
            for record in lineage[1:target_index + 1]:
                for i, value in enumerate(list(record.columns)):
                    if value is not None:
                        merged_columns[i] = value
                        
            record = lineage[target_index]
            if merged_columns[aggregate_column_index] is None:
                continue
            range_sum += merged_columns[aggregate_column_index]
        
        return range_sum
      

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
