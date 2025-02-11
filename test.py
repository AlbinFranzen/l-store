from lstore.table import Table
from lstore.query import Query

test_table = Table("test_table", 3, 0)
query = Query(test_table)
query.insert(50, 2, 3)
query.insert(51, 4, 6)
query.insert(20, 3, 4)
query.update(51, None, None, 10)
query.update(50, 5, None)

#print(query.sum_version(0, 30, 1, -1))
print(query.select(51, 0, [1, 1, 1]))
print(query.sum)
#print(query.select_version(50, 0, [1, 1, 1], -1)[0])

