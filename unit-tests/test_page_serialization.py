import unittest
from lstore.page import Page
from lstore.table import Record


class TestPageSerialization(unittest.TestCase):
    def setUp(self):
        """Set up test cases"""
        self.page = Page()
        
        # Create some test records
        self.test_records = [
            Record(None, "b0", 1234567890, [0, 0, 0], [1, 2, 3]),
            Record("b0", "t0", 1234567891, [1, 1, 1], [4, 5, 6]),
            Record("t0", "t1", 1234567892, [0, 1, 0], [7, 8, 9])
        ]

    def test_empty_page_serialization(self):
        """Test serialization of empty page"""
        # Serialize empty page
        serialized = self.page.serialize()
        
        # Deserialize and verify
        deserialized_page = Page.deserialize(serialized)
        self.assertEqual(deserialized_page.num_records, 0)
        self.assertEqual(len(deserialized_page.data), 0)

    def test_single_record_serialization(self):
        """Test serialization with single record"""
        # Add one record
        self.page.write(self.test_records[0])
        
        # Serialize and deserialize
        serialized = self.page.serialize()
        deserialized_page = Page.deserialize(serialized)
        
        # Verify record count
        self.assertEqual(deserialized_page.num_records, 1)
        
        # Verify record contents
        original_record = self.test_records[0]
        deserialized_record = deserialized_page.data[0]
        
        self.assertEqual(deserialized_record.indirection, original_record.indirection)
        self.assertEqual(deserialized_record.rid, original_record.rid)
        self.assertEqual(deserialized_record.start_time, original_record.start_time)
        self.assertEqual(deserialized_record.schema_encoding, original_record.schema_encoding)
        self.assertEqual(deserialized_record.columns, original_record.columns)

    def test_multiple_records_serialization(self):
        """Test serialization with multiple records"""
        # Add multiple records
        for record in self.test_records:
            self.page.write(record)
            
        # Serialize and deserialize
        serialized = self.page.serialize()
        deserialized_page = Page.deserialize(serialized)
        
        # Verify record count
        self.assertEqual(deserialized_page.num_records, len(self.test_records))
        
        # Verify all records
        for i, original_record in enumerate(self.test_records):
            deserialized_record = deserialized_page.data[i]
            self.assertEqual(deserialized_record.indirection, original_record.indirection)
            self.assertEqual(deserialized_record.rid, original_record.rid)
            self.assertEqual(deserialized_record.start_time, original_record.start_time)
            self.assertEqual(deserialized_record.schema_encoding, original_record.schema_encoding)
            self.assertEqual(deserialized_record.columns, original_record.columns)

    def test_serialization_with_none_values(self):
        """Test serialization handling of None values"""
        # Create record with None values
        record_with_nones = Record(None, "b1", None, [None, None], [None, None, None])
        self.page.write(record_with_nones)
        
        # Serialize and deserialize
        serialized = self.page.serialize()
        deserialized_page = Page.deserialize(serialized)
        
        # Verify None values are preserved
        deserialized_record = deserialized_page.data[0]
        self.assertIsNone(deserialized_record.indirection)
        self.assertIsNone(deserialized_record.start_time)
        self.assertEqual(deserialized_record.schema_encoding, [None, None])
        self.assertEqual(deserialized_record.columns, [None, None, None])

    def test_large_page_serialization(self):
        """Test serialization of page with maximum records"""
        from lstore.config import PAGE_RECORD_SIZE
        
        # Fill page to capacity
        for i in range(PAGE_RECORD_SIZE):
            record = Record(None, f"b{i}", 1234567890 + i, [0, 0, 0], [i, i+1, i+2])
            self.page.write(record)
            
        # Serialize and deserialize
        serialized = self.page.serialize()
        deserialized_page = Page.deserialize(serialized)
        
        # Verify record count
        self.assertEqual(deserialized_page.num_records, PAGE_RECORD_SIZE)
        
        # Verify first and last records
        self.assertEqual(deserialized_page.data[0].rid, "b0")
        self.assertEqual(deserialized_page.data[-1].rid, f"b{PAGE_RECORD_SIZE-1}")

if __name__ == '__main__':
    unittest.main() 
