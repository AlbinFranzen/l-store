from lstore.table import Table
from lstore.query import Query

test_table = Table("test_table", 3, 0)
query = Query(test_table)
query.insert(50, 2, 3)
print(query.table.index.locate(0, 50))
#
#query.update(50, None, 10)
#query.update(50, 5, None)

#print(query)
#print(query._traverse_lineage("b0")[0].rid)
#print(query.select(5, 0, [1, 1, 1])[0].columns)

