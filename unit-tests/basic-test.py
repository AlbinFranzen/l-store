import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lstore.query import Query
from lstore.db import Database

db = Database()
db.open('./ECS165')
test_table = db.create_table("test_table", 3, 0)
query = Query(test_table)

query.insert(50, 2, 3)
query.insert(51, 4, 6)

#query.update(50, None, 6, None)
query.update(51, None, None, 10)
query.update(51, None, 5, None)

test_table.merge(0)

db.close()
test_table.merge_thread.join()

db.open('./ECS165')

new_test_table = db.get_table('test_table')
query = Query(new_test_table)

for path, offset in query.table.page_directory.values():
    print(f"record: {query.table.bufferpool.get_page(path).read_index(offset)}")
    
print(new_test_table.index.indices)

# # b0_path, offset = new_test_table.page_directory["b0"][0]
# # print(f"b0 path: {b0_path}")
# # b0_record = new_test_table.bufferpool.get_page(b0_path).read_index(offset)
# # print(f"b0 record: {b0_record}")
# # print("Search: ", new_test_table.index.locate(6,0))
# print(f"\nPage directory: {new_test_table.page_directory}")
# query.select_version(50, 0, [1, 1, 1], -1)
# query.select_version(51, 0, [1, 1, 1], -1)
# print(f"\nquery select: {query.select(51, 0, [1, 1, 1])} ")
# base_records = test_table.bufferpool.get_page(os.path.join(test_table.path, "pagerange_0/base/page_0")).read_all()
# tail_records = test_table.bufferpool.get_page(os.path.join(test_table.path, "pagerange_0/tail/page_0")).read_all()
#print(f"base recs: {base_records}\ntail records: {tail_records}")
db.close()




