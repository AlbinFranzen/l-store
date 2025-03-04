import os
import time
import copy
from lstore.table import Record
from lstore.page import Page
from lstore.config import MERGE_THRESH, PAGE_RANGE_SIZE

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table


    def __repr__(self):
        return f"Table:\n{self.table}"
    
    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        # Locate base record
        base_rid = self.table.index.locate(0, primary_key)
        if base_rid is False or not base_rid:
            return False
        if isinstance(base_rid, bytes):
            base_rid = base_rid.decode()

        # Retrieve base record
        base_path, base_offset = self.table.page_directory[base_rid]
        base_record = self.table.bufferpool.get_page(base_path).read_index(base_offset)
        self.table.bufferpool.unpin_page(base_path)

        # Get last tail record
        last_tail_path, last_tail_offset = self.table.page_directory[base_record.indirection]
        last_tail_record = self.table.bufferpool.get_page(last_tail_path).read_index(last_tail_offset)
        self.table.bufferpool.unpin_page(last_tail_path)

        # Create deletion marker record
        record = Record(
            base_record.rid,
            last_tail_record.rid,
            f"t{self.table.current_tail_rid}",
            time.time(),
            [0] * len(base_record.schema_encoding),
            [None] * len(base_record.columns)
        )

        # Get pagerange index from base path
        base_pagerange_index = int(base_path.split("pagerange_")[1].split("/")[0])

        tail_path = self.table.tail_page_locations[base_pagerange_index]
        tail_page = self.table.bufferpool.get_page(tail_path)

        # Write to appropriate page
        if tail_page.has_capacity():
            tail_page.write(record)
            insert_path, offset = tail_path, tail_page.num_records - 1
        else:
            new_path = f"{self.table.path}/pagerange_{base_pagerange_index}/tail/page_{self.table.tail_page_indices[base_pagerange_index] + 1}"
            new_page = Page()
            new_page.write(record)
            self.table.bufferpool.add_frame(new_path, new_page)
            self.table.tail_page_locations[base_pagerange_index] = new_path
            self.table.tail_page_indices[base_pagerange_index] += 1
            insert_path, offset = new_path, 0

        # Update metadata
        self.table.bufferpool.unpin_page(tail_path)
        self.table.page_directory[record.rid] = [insert_path, offset]
        self.table.index.add_record(record)
        self.table.current_tail_rid += 1

        # Handle merge threshold
        self.table.pr_unmerged_updates[base_pagerange_index] += 1
        if self.table.pr_unmerged_updates[base_pagerange_index] >= MERGE_THRESH:
            self.table.merge(base_pagerange_index)

        return True


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    # FOR BASE PAGES
    """
    def insert(self, *columns):
        if not self._verify_insert_input(*columns):
            return False
        record = Record(f"b{self.table.current_base_rid}", f"b{self.table.current_base_rid}", f"b{self.table.current_base_rid}", time.time(), [0] * len(columns), [*columns])
        self.table.index.add_record(record)
        
        last_path = self.table.base_page_locations[len(self.table.base_page_locations) - 1]
        last_page = self.table.bufferpool.get_page(last_path)

        last_page = self.table.bufferpool.get_page(last_path)
        self.table.bufferpool.unpin_page(last_path)
        last_pagerange_index, last_page_index = self._parse_page_path(last_path)
        
        if last_page.has_capacity():
            last_page.write(record)
            self.table.bufferpool.update_page(last_path, make_dirty=True)
            self.table.base_page_locations[last_pagerange_index] = last_path
            insert_path = self.table.last_path
            offset = last_page.num_records - 1
        else:
            new_page = Page() 
            new_page.write(record)
            self.table.bufferpool.update_page(last_path, make_dirty=True)

            insert_path = last_path
            offset = 0
            if last_page_index + 1 < PAGE_RANGE_SIZE:
                insert_path = f"{self.table.path}/pagerange_{last_pagerange_index}/base/page_{last_page_index + 1}"
                self.table.base_page_locations[last_pagerange_index] = insert_path
                self.table.pr_unmerged_updates.append(0)
            else:
                new_pagerange_path = f"{self.table.path}/pagerange_{last_pagerange_index + 1}"
                os.makedirs(f"{new_pagerange_path}/base", exist_ok=True)
                os.makedirs(f"{new_pagerange_path}/tail", exist_ok=True)
                insert_path = f"{new_pagerange_path}/base/page_0"
                self.table.page_range_tps[last_pagerange_index + 1] = 0
                self.table.base_page_locations.append(insert_path)
                self.table.original_per_page_range.append([0]*PAGE_RANGE_SIZE)
                self.table.tail_page_locations.append(f"{new_pagerange_path}/tail/page_0")
                self.table.tail_page_indices.append(0)
                first_tail_page = Page()
                self.table.bufferpool.add_frame(f"{new_pagerange_path}/tail/page_0", first_tail_page)
    
        
            self.table.last_path = insert_path
            self.table.bufferpool.add_frame(insert_path, new_page)
        
        self.table.page_directory[f"b{self.table.current_base_rid}"] = [insert_path, offset]
        self.table.current_base_rid += 1
        return True
    
    
    def _verify_insert_input(self, *columns):
        for column in columns:
            if not isinstance(column, int):
                return False
        return True


    def _parse_page_path(self, path):
        # Extract pagerange index and page index from a path
        try:
            pagerange_part = path.split("pagerange_")[1]
            pagerange_index = int(pagerange_part.split("/")[0])
            page_part = path.split("page_")[1]
            # Remove potential file extension if present.
            page_index = int(page_part.split("/")[0].split('.')[0])
            return pagerange_index, page_index
        except Exception as e:
            print("Error parsing page path:", e)
            return 0, 0

    
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
            base_path, base_offset = self.table.page_directory[rid]
            base_record = self.table.bufferpool.get_page(base_path).read_index(base_offset)
            self.table.bufferpool.unpin_page(base_path)
            
            tail_path, tail_offset = self.table.page_directory[base_record.indirection]
            tail_record = self.table.bufferpool.get_page(tail_path).read_index(tail_offset)
            self.table.bufferpool.unpin_page(tail_path)
            
            new_record = copy.deepcopy(tail_record)
            new_record.columns = [element for element, bit in zip(tail_record.columns, projected_columns_index) if bit == 1]
            records.append(new_record)
        return records if records else False

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
            
            print("No records found", search_key, search_key_index)
            return None
        rid_list = rids_combined.split(",")
        # Here, each element in record_lineages is already a lineage (a list of records)
        results = []
        for rid in rid_list:
            temp_rid = rid
            for i in range(abs(relative_version-2)):
                temp_record_path, offset = self.table.page_directory[temp_rid]
                temp_record = self.table.bufferpool.get_page(temp_record_path).read_index(offset)
                temp_rid = temp_record.indirection  
                if temp_rid == temp_record.base_rid:
                    break
                
            modified_record = copy.deepcopy(temp_record) 
            modified_record.columns = [element for element, bit in zip(temp_record.columns, projected_columns_index) if bit == 1]
            results.append(temp_record)
            
        return results


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    # FOR TAIL PAGES
    """
    def update(self, primary_key, *columns):
        base_rid = self.table.index.locate(0, primary_key)
        if base_rid is False or not base_rid:
            return False
        if isinstance(base_rid, bytes):
            base_rid = base_rid.decode()

        # Retrieve base record
        base_path, base_offset = self.table.page_directory[base_rid]
        base_record = self.table.bufferpool.get_page(base_path).read_index(base_offset)
        self.table.bufferpool.unpin_page(base_path)

        is_first_update = base_record.indirection == base_record.rid

        if not is_first_update:
            last_tail_path, last_tail_offset = self.table.page_directory[base_record.indirection]
            last_tail_record = self.table.bufferpool.get_page(last_tail_path).read_index(last_tail_offset)
            self.table.bufferpool.unpin_page(last_tail_path)
        else:
            # Extract pagerange index from base_path efficiently
            base_pagerange_index = int(base_path.split("pagerange_")[1].split("/")[0])
            original_copy = Record(
                base_record.rid,
                base_record.rid,
                f"t{self.table.current_tail_rid}",
                time.time(),
                [1 if col is not None else 0 for col in columns],
                base_record.columns
            )
            self.table.current_tail_rid += 1

            current_tail_path = self.table.tail_page_locations[base_pagerange_index]
            current_tail_page = self.table.bufferpool.get_page(current_tail_path)
            
            # Handle page capacity
            if current_tail_page.has_capacity():
                current_tail_page.write(original_copy)
                self.table.bufferpool.update_page(current_tail_path, make_dirty=True)
                insert_path, offset = current_tail_path, current_tail_page.num_records - 1
            else:
                new_path = f"{self.table.path}/pagerange_{base_pagerange_index}/tail/page_{self.table.tail_page_indices[base_pagerange_index] + 1}"
                new_page = Page()
                new_page.write(original_copy)
                self.table.bufferpool.add_frame(new_path, new_page)
                self.table.bufferpool.update_page(current_tail_path, make_dirty=True)
                self.table.tail_page_locations[base_pagerange_index] = new_path
                self.table.tail_page_indices[base_pagerange_index] += 1
                insert_path, offset = new_path, new_page.num_records - 1

            self.table.page_directory[original_copy.rid] = [insert_path, offset]
            last_tail_record = original_copy
            self.table.bufferpool.unpin_page(current_tail_path)
        
        # Prepare new record
        schema_len = len(columns)
        new_schema = []
        new_cols = []
        for i in range(schema_len):
            if columns[i] is not None:
                new_schema.append(1)
                new_cols.append(columns[i])
            else:
                new_schema.append(last_tail_record.schema_encoding[i])
                new_cols.append(last_tail_record.columns[i])

        record = Record(
            base_record.rid,
            last_tail_record.rid,
            f"t{self.table.current_tail_rid}",
            time.time(),
            new_schema,
            new_cols
        )

        # Update base record pointers
        base_record.schema_encoding = new_schema
        base_record.indirection = record.rid

        # Write new tail record
        base_pagerange_index = int(base_path.split("pagerange_")[1].split("/")[0])
        
        current_tail_path = self.table.tail_page_locations[base_pagerange_index]
        current_tail_page = self.table.bufferpool.get_page(current_tail_path)

        if current_tail_page.has_capacity():
            current_tail_page.write(record)
            self.table.bufferpool.update_page(current_tail_path, make_dirty=True)
            insert_path, offset = current_tail_path, current_tail_page.num_records - 1
        else:
            new_path = f"{self.table.path}/pagerange_{base_pagerange_index}/tail/page_{self.table.tail_page_indices[base_pagerange_index] + 1}"
            self.table.tail_page_indices[base_pagerange_index] += 1
            self.table.tail_page_locations[base_pagerange_index] = new_path
            
            new_page = Page()
            new_page.write(record)
            self.table.bufferpool.add_frame(new_path, new_page)
            self.table.bufferpool.update_page(new_path, make_dirty=True)
            insert_path, offset = new_path, 0

        self.table.page_directory[record.rid] = [insert_path, offset]
        self.table.current_tail_rid += 1

        # Merge logic
        self.table.pr_unmerged_updates[base_pagerange_index] += 1
        if self.table.pr_unmerged_updates[base_pagerange_index] >= MERGE_THRESH:
            self.table.merge(base_pagerange_index)
            
        self.table.bufferpool.unpin_page(current_tail_path)
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
        # Use locate_range to obtain a dictionary mapping keys to decoded RID strings
        rid_dict = self.table.index.locate_range(start_range, end_range, 0)
        if not rid_dict:
            return False
        # Get the list of RID strings from the dictionary values
        rids = list(rid_dict.values())
        range_sum = 0
        for rid in rids:
            base_path, base_offset = self.table.page_directory[rid]
            base_record = self.table.bufferpool.get_page(base_path).read_index(base_offset)
            self.table.bufferpool.unpin_page(base_path)
            
            last_path, last_offset = self.table.page_directory[base_record.indirection]
            last_record = self.table.bufferpool.get_page(last_path).read_index(last_offset)
            self.table.bufferpool.unpin_page(last_path)
            
            if last_record and last_record.columns[aggregate_column_index] is not None:
                range_sum += last_record.columns[aggregate_column_index]
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
        rids = list(rids.values())
        
        for rid in rids:     
            temp_rid = rid
            for i in range(abs(relative_version-2)):
                temp_record_path, offset = self.table.page_directory[temp_rid]
                temp_record = self.table.bufferpool.get_page(temp_record_path).read_index(offset)
                self.table.bufferpool.unpin_page(temp_record_path)
                temp_rid = temp_record.indirection
                       
            range_sum += temp_record.columns[aggregate_column_index]
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
