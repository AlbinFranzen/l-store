from uuid import uuid4
from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
from lstore.page_range import PageRange
from lstore.config import *
import time
import copy

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
        self.current_key = 0
        pass
    
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
        
        # get all records in lineage
        lineage = self._traverse_lineage(base_rid[0])

        # create a new tail record
        record = Record(
                        lineage[0].rid,
                        "t" + str(self.current_tail_rid),
                        time.time(),
                        [0] * (len(lineage[0].schema_encoding) - 1),
                        [None] * len(lineage[0].columns)
                    )
        
        # update base record's schema encoding
        lineage[0].schema_encoding = record.schema_encoding
        lineage[-1].indirection = record.rid

        # ensure space exists
        if not self.table.page_ranges[-1].tail_pages[-1].has_capacity():
            self.table.page_ranges[-1].tail_pages.append(Page())
        if not self.table.page_ranges[-1].has_capacity():
            self.table.page_ranges.append(PageRange())
        
        # add record in index
        self.table.index.add_record(record)

        # insert tail record
        offset = self.table.page_ranges[-1].tail_pages[-1].write(record)
        tail_page_index = len(self.table.page_ranges[-1].tail_pages) - 1
        page_range_index = len(self.table.page_ranges) - 1

        # add new location to page directory
        self.table.page_directory[lineage[0].rid].append([page_range_index, tail_page_index, offset])

        # update tail rid
        self.update_tail_rid()

        return True
        pass
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    # FOR BASE PAGES
    """
    def insert(self, *columns):
         # Verify input
        if not self._verify_insert_input(*columns):
            return False
        
        # Create record
        record = Record(None, f"b{self.current_base_rid}", time.time(), [0] * (len(columns) + 1), [self.current_key, *columns])
        
        # Make sure space exists
        self.table.index.add_record(record)
        if not self.table.page_ranges[-1].has_capacity(): # If page range is full, create new one
            self.table.page_ranges.append(PageRange())
        if not self.table.page_ranges[-1].base_pages[-1].has_capacity(): # If base page is full, create new one
            self.table.page_ranges[-1].base_pages.append(Page())
            
        # Write and get location 
        offset = self.table.page_ranges[-1].base_pages[-1].write(record) 
        base_page_index = len(self.table.page_ranges[-1].base_pages) - 1
        page_range_index = len(self.table.page_ranges) - 1 
        
        # Add new location to page directory 
        self.table.page_directory[f"b{self.current_base_rid}"] = [[page_range_index, base_page_index, offset]]
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
        rid_list = self.table.index.locate(search_key_index, search_key)
        rid_list = ["b0"]
        if rid_list == False:
            return False
        # Get the corresponding records
        records = [self._traverse_lineage(rid)[-1] for rid in rid_list]

        # Convert records to only have the projected columns
        output_records = []
        if all(projected_columns_index):
            return records   
        for record in records:   
            new_record = Record(record.rid, record.indirection, record.time_stamp, record.schema_encoding, [col for col, include in zip(record.columns[1:], projected_columns_index) if include])
            output_records.append(new_record)     
        return output_records
    
    # Get list of records from base_rid
    def _traverse_lineage(self, base_rid):
        lineage = []
        page_entries = self.table.page_directory[base_rid]
        for i, (page_range_index, page_index, offset) in enumerate(page_entries): # Loop through the page entries
            page_range = self.table.page_ranges[page_range_index]
            if i == 0: # First record comes from the base page.   
                current_record = page_range.base_pages[page_index].read_index(offset)
            else: # Subsequent records come from tail pages.
                current_record = page_range.tail_pages[page_index].read_index(offset)        
            lineage.append(current_record)
            if current_record.indirection is None or current_record.indirection == base_rid:
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
        records = self.select(search_key, search_key_index, projected_columns_index)
        lineages = []
        for record in records:
            lineages.append(self._traverse_lineage(record.rid))
        
        results = []
        for lineage in lineages:
            try:
                record = lineage[relative_version]
            except IndexError:
                return False
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
        base_rid = ["b0"]
        if base_rid == False:
            return False
        
        # Get all records in lineage
        lineage = self._traverse_lineage(base_rid[0])
        
        # Create new record
        record = Record(lineage[0].rid, "t" + str(self.current_tail_rid), time.time(), [1 if update is not None else orig for orig, update in zip(lineage[0].schema_encoding, [lineage[-1].columns[0], *columns])], [upd if upd is not None else orig for orig, upd in zip(lineage[-1].columns, [lineage[-1].columns[0], *columns])])
        # Update old records
        lineage[0].schema_encoding = record.schema_encoding # update base record's schema encoding
        lineage[-1].indirection = record.rid   # tail record's rid
        
        # Make sure space exists
        self.table.index.add_record(record)
        if not self.table.page_ranges[-1].has_capacity(): # If page range is full, create new one
            self.table.page_ranges.append(PageRange())
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
        range_sum = 0
        record_exists = False
        key_range = self.index.locate_range(start_range, end_range)

        for primary_key in key_range:
            record = self.index.get_record(primary_key)
            range_sum += record.columns[aggregate_column_index]
            record_exists = True

        if record_exists:
            return range_sum
        else:
            return False
        
    
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
        record_exists = False
        rids = self.index.locate_range(start_range, end_range, 0)

        #traverse tree for each rid found in range
        for rid in rids:
            lineage = self._traverse_lineage(rid)
            
            #checks to make sure version exists
            if abs(relative_version) > len(lineage):
                continue

            record = lineage[relative_version]
            range_sum += record.columns[aggregate_column_index]
            record_exists = True

        if record_exists:
            return range_sum
        else:
            return False

    
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
