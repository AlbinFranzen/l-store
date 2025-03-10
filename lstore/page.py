import msgpack
from lstore.config import PAGE_RECORD_SIZE
import threading

class Page:
    def __init__(self):
        self.num_records = 0
        self.data = []
        self._write_lock = threading.RLock()

    def __repr__(self):
        return f"num_records: {self.num_records}  |  data: {self.data}"

    def has_capacity(self):  # Check if page has capacity
        return self.num_records < PAGE_RECORD_SIZE

    def write(self, record):  # Append record
        with self._write_lock:
            if not self.has_capacity():
                return
            self.data.append(record)
            self.num_records += 1
            return self.num_records-1
    
    def overwrite_index(self, index, record):  # Overwrite record at index
        self.data[index] = record

    def overwrite_rid(self, index, value):  # Overwrite the rid at index
        self.data[index].rid = value

    def read_all(self):  # Read all records
        return self.data

    def read_index(self, index):  # Read record at index
        return self.data[index]

    def serialize(self):
        """
        Serialize page data into bytes for disk storage
        Returns:
            bytes: Serialized page data
        """
        page_data = {
            'num_records': self.num_records,
            'records': []
        }
        
        # Serialize each record
        for record in self.data:
            record_data = {
                'base_rid': record.base_rid,
                'indirection': record.indirection,
                'rid': record.rid,
                'start_time': record.start_time,
                'schema_encoding': record.schema_encoding,
                'columns': record.columns
            }
            page_data['records'].append(record_data)
        return msgpack.packb(page_data)

    @classmethod 
    def deserialize(cls, data):
        """
        Deserialize bytes data into a Page object
        Args:
            data (bytes): Serialized page data
        Returns:
            Page: Reconstructed page object
        """
        # Import here to avoid circular import
        from lstore.table import Record
        
        # Create new page
        page = cls()
        
        # Unpack the serialized data
        page_data = msgpack.unpackb(data)
        
        # Set page metadata
        page.num_records = page_data['num_records']
        
        # Reconstruct records
        for record_data in page_data['records']:
            record = Record(
                record_data['base_rid'],
                record_data['indirection'],
                record_data['rid'], 
                record_data['start_time'],
                record_data['schema_encoding'],
                record_data['columns']
            )
            page.data.append(record)
        return page
