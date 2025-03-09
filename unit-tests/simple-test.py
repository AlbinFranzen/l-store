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
trans1.add_query(query.insert, test_table, 3006, 1, 1, 1, 1)

trans2 = Transaction()
trans2.add_query(query.insert, test_table, 3006, 0, 0, 0, 0)
worker1 = TransactionWorker()
worker2 = TransactionWorker()
worker1.add_transaction(trans1)
worker2.add_transaction(trans2)
for worker in worker1, worker2:
    worker.run()



worker1.join()
worker2.join()

print("Table directory:", test_table.page_directory)
path1, offset1 = test_table.page_directory['b0']
path2, offset2 = test_table.page_directory['b1']

print("Record 1:", test_table.bufferpool.get_page(path1).read_index(offset1))
print("Record 2:", test_table.bufferpool.get_page(path2).read_index(offset2))

