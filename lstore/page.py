class Page:
    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return (self.num_records < 512) # If fewer records than 512, we have capacity to write

    def write(self, value): # Assume value is 8 bytes (64-bit integer)
       self.data[self.num_records * 8 : self.num_records * 8 + 8] = value  # Write value to the next available slot
       self.num_records += 1
       pass
