from lstore.table import Table
from lstore.query import Query

test_table = Table("test_table", 3, 0)
query = Query(test_table)
query.insert(50, 2, 3)
query.insert(51, 4, 6)
query.insert(51, 3, 4)
print(query.table.index.locate(0, 51))
print(query.table.index.locate_range(20,60,0))
print(query.table.index.locate_range(100,120,0))

#query.update(50, None, 10)
#query.update(50, 5, None)

#print(query)
#print(query._traverse_lineage("b0")[0].rid)
#print(query.select(5, 0, [1, 1, 1])[0].columns)

