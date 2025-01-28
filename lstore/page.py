class Page:
    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return (self.num_records < 512) # If fewer records than 512, we have capacity to write

    def append(self, value): # Assume value is 8 bytes (64-bit integer)
       self.data[self.num_records * 8 : self.num_records * 8 + 8] = value  # Write value to the next available slot
       self.num_records += 1
       pass
    
    def overwrite(self, index, value): # Assume value is 8 bytes (64-bit integer), used for indirection
        self.data[index * 8 : index * 8 + 8] = value  # Overwrite value at index
        pass
    
    def read_all(self):
        return self.data
    
    def read_index(self, index):
        return self.data[index * 8 : index * 8 + 8]
