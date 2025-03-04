import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lstore.query import Query
from lstore.db import Database

db = Database()
db.open('./ECS165')
test_table = db.create_table("test_table", 3, 0)
query = Query(test_table)

print(test_table.bufferpool)

for i in range(1):
    query.insert(i, 2, 3)
    
for i in range (1):
    query.update(i, None, 6, None)
    
for i in range(1):
    query.delete(i)

print(test_table.bufferpool)


db.close()





