from lstore.table import Table
from lstore.query import Query

test_table = Table("test_table", 3, 0)
query = Query(test_table)
query.insert(1, 2, 3)
print(test_table.page_ranges[0].base_pages[0].data[0].columns)
