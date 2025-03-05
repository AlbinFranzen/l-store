import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lstore.query import Query
from lstore.db import Database

db = Database()
db.open('./ECS165')
test_table = db.create_table("test_table", 3, 0)
query = Query(test_table)

for i in range(0, 10):
    query.insert(i, i, i)

for i in range(0, 25):
    query.update(0, None, None, 10*i)


for i in range(0, 10):
    print(query.select(i, 0, [1, 1, 1]))

# print("Page Directory:")
# for key, value in test_table.page_directory.items():
#     print(f"Key: {key}, Value: {value}")


#for i in range(0, 10):
   # print(query.select(i, 0, [1, 1, 1]))
    

#print(query.select(50, 0, [1, 1, 1]))

db.close()





