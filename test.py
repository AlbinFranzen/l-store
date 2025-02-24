from lstore.table import Table
from lstore.query import Query
from lstore.db import Database

db = Database()
test_table = db.create_table("test_table", 3, 0)
# db.get_table("test_table")
# test_table = Table("test_table", 3, 0)
query = Query(test_table)
query.insert(50, 2, 3)
query.insert(51, 4, 6)
# query.insert(20, 3, 4)
query.update(51, None, None, 10)
# query.delete(51)
query.update(51, None, 5, None)
# query.update(20, 10, 5, None)
# query.update(20, 15, 5, None)

#print(query.sum_version(0, 30, 1, -1))
print(query.table.page_directory)
print("")
print(query.select(51, 0, [1, 1, 1]))
#print(query.sum_version(5, 60, 0))
#print(query.select_version(50, 0, [1, 1, 1], -1)[0])

