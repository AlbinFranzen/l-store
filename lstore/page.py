from lstore.config import PAGE_RECORD_SIZE

class Page:
    def __init__(self):
        self.num_records = 0
        self.data = []

    def __repr__(self):
        return f"num_records: {self.num_records}  |  data: {self.data}"

    def has_capacity(self):  # Check if page has capacity
        return self.num_records < PAGE_RECORD_SIZE

    def write(self, record):  # Append record
        if not self.has_capacity():
            # print("Maximum capacity of PAGE_RECORD_SIZE records has been reached.")
            return
        self.data.append(record)
        self.num_records += 1
        return self.num_records-1

    def overwrite_rid(self, index, value):  # Overwrite the rid at index
        self.data[index].rid = value
        pass

    def read_all(self):  # Read all records
        return self.data

    def read_index(self, index):  # Read record at index
        return self.data[index]
