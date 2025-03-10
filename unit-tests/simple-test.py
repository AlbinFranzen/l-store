import threading
import time
import os
import sys
import argparse
import shutil
import unittest
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
from lstore.two_phase_lock import TwoPhaseLock, LockMode, LockGranularity

db = Database()
db.open('./ECS165')
test_table = db.create_table('test', 5, 0) #test name 5 col key 0
query = Query(test_table)
query.insert(3005, 1, 2, 3, 4) #rid is b0 (yes i had to write this im stupid)
query.insert(3006, 5, 6, 7, 8)

trans1 = Transaction()
trans1.add_query(query.update, test_table, 3006, None, 1, 1, 1, 1)

trans2 = Transaction()
trans2.add_query(query.update, test_table, 3006, None, 0, 0, 0, 0)
worker1 = TransactionWorker()
worker2 = TransactionWorker()
worker1.add_transaction(trans1)
worker2.add_transaction(trans2)
for worker in worker1, worker2:
    worker.run()

worker1.join()
worker2.join()

print("Table directory:", test_table.page_directory)

for rid, location in test_table.page_directory.items():
    print(f"Record {rid}: {test_table.bufferpool.get_page(location[0]).read_index(location[1])}")


