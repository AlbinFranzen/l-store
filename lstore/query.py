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
        self.current_rid = 0
        self.current_key = 0
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    # FOR BASE PAGES
    """
    def insert(self, *columns):
        if not self.verify_insert_input(*columns):
            return False
        record = Record(self.current_rid, self.current_key, time.time(), 0, columns) # Create record instance
        page_range_index, base_page_index, offset = self.table.insert_record(record) # Insert record to the table and update metadata
        self.table.page_directory[self.current_rid] = [[page_range_index, base_page_index, offset]] # Add new instance to directory
        self.current_rid += 1
        self.current_key += 1
        return True
    
    def verify_insert_input(self, *columns):
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
        pass

    
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
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    # FOR TAIL PAGES
    """
    def update(self, primary_key, *columns):
        # check if the record exists in the table
        if primary_key not in self.table.page_directory:
            return False
        
        # turn *columns into a list
        updated_columns = list(columns)
        # get the base record object
        baserecordOJ = self.index.get_record(primary_key)
        # get the current tail page
        cur_tail_page = self.page_range.get_tail_page()

        # check if the tail page is full
        if not cur_tail_page.tail_page_has_capacity():
            # create a new tail page
            new_tail_page = Page()
            cur_tail_page = new_tail_page
            # add tail_page in the page range

        # if it is the first time updating the base_record
        if(baserecordOJ.indirection == None):
            # create the first tail record
            first_tail_record = copy.deepcopy(baserecordOJ)
            first_tail_record.rid = uuid4()
            first_tail_record.indirection = baserecordOJ.rid
            first_tail_record.time_stamp = time.time()
            first_tail_record.schema_encoding = 

            for i in range(len(updated_columns)):
                first_tail_record.columns[i] = updated_columns[i]

            baserecordOJ.indirection = first_tail_record.rid

            # write the first tail record to the tail page
            cur_tail_page.insert_record(first_tail_record)

        # if the base record has been updated before
        recent_tail_record = self.index.get_record(baserecordOJ.indirection)
        # create a new tail record
        new_tail_record = copy.deepcopy(recent_tail_record)
        new_tail_record.rid = uuid4()
        new_tail_record.time_stamp = time.time()
        new_tail_record.schema_encoding =
        new_tail_record.indirection = recent_tail_record.rid

        for i in range(len(updated_columns)):
            new_tail_record.columns[i] = updated_columns[i]

        baserecordOJ.indirection = new_tail_record.rid

        # write the new tail record to the tail page
        cur_tail_page.insert_record(new_tail_record)

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
        total_sum = 0
        record_exists = False
        for primary_key in range(start_range, end_range + 1):
            if primary_key in self.table.page_directory:
                record = self.index.get_record(primary_key)
                total_sum += record.columns[aggregate_column_index]
                record_exists = True
        if record_exists:
            return total_sum
        else:
            return False
        
        pass

    
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
        pass

    
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
