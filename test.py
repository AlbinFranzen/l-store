from lstore.table import Table
from lstore.query import Query

test_table = Table("test_table", 3, 0)
query = Query(test_table)
query.insert(5, 2, 3)
query.update(0, None, 10, 10)
query.update(0, 7, 5, None)
#print(query.table.page_ranges[0].base_pages[0].read_index(0).indirection)
#print(query.table.page_directory)
#print(query._traverse_lineage("b0")[0].rid)
print(query.select(5, 0, [1, 1, 1])[0].columns)

