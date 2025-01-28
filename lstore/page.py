class Page:
    def __init__(self):
        self.num_records = 0
        self.data = []

    def has_capacity(self): # Check if page has capacity
        return (self.num_records < 512) 

    def append(self, record): # Append record
       self.data.append(record)
       self.num_records += 1
       pass
    
    def overwrite_rid(self, index, value): # Overwrite the rid at index
        self.data[index].rid = value  
        pass
    
    def read_all(self): # Read all records
        return self.data
    
    def read_index(self, index): # Read record at index
        return self.data[index]
