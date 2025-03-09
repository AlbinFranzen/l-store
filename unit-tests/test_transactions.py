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

class testingTransactions(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        self.db.open('./ECS165')
        self.test_table = self.db.create_table('test', 5, 0) #test name 5 col key 0
        self.query = Query(self.test_table)
        self.query.insert(3005, 1, 2, 3, 4) #rid is b0 (yes i had to write this im stupid)
        self.query.insert(3006, 5, 6, 7, 8)
        
    
    def tearDown(self):
        # Clean up the database and reset any shared state
        self.db.close()
        self.db = None
        self.test_table = None
        self.query = None
        Transaction.global_lock_manager = None  # Reset the global lock manager
        Transaction.transaction_id_counter = 0  # Reset the transaction ID counter
        Transaction.transaction_id_lock = threading.Lock()  # Reset the transaction ID lock
        Transaction.global_lock_manager_lock = threading.Lock()  # Reset the global lock manager lock
        if os.path.exists('./ECS165'):
            shutil.rmtree('./ECS165')
        

    def test_TransactionOBJ(self):
        #tests object created
        trans = Transaction()
        self.assertIsNotNone(trans, msg="failed to create transaction")
        #tests if lock manager created once
        trans1 = Transaction()
        self.assertEqual(trans.lock_manager, trans1.lock_manager, msg="multiple lock managers created")
        self.assertEqual(trans.lock_manager, Transaction.global_lock_manager, msg="somehow lock manager equal to class")
        
        #are they supposed to be equal?
        #check transaction id locks are different
        #self.assertNotEqual(trans.transaction_id_lock, trans1.transaction_id_lock, msg="transaction locks are the same")
        
        #checks if query is added correctly
        trans.add_query(self.query.insert, self.test_table, 3005, 2, 3, 4, 5)
        self.assertIsNotNone(trans.queries, msg="query failed to add")
        exQ = []
        exQ.append((self.query.insert, self.test_table, (3005,2,3,4,5)))
        self.assertEqual(trans.queries, exQ, msg="query formatted incorrectly")
        

    def test_RunInsertLocks(self):
        #assert granularity order
        trans1 = Transaction()
        trans2 = Transaction()

        def acquire_insert_locks(transaction):
            res = transaction._acquire_insert_locks(self.test_table, LockMode.EXCLUSIVE)
            if res:
                self.assertEqual(transaction.held_locks[self.test_table.name], (LockGranularity.TABLE, LockMode.EXCLUSIVE), msg="lock not held correctly")
            try:
                self.assertTrue(res, msg=f"lock not acquired for thread: {threading.current_thread().name}")
            except AssertionError as e:
                print(f"AssertionError in thread {threading.current_thread().name}: {str(e)}")
            
        # Create two threads that try to acquire insert locks at the same time
        thread1 = threading.Thread(target=acquire_insert_locks, args=(trans1,))
        thread2 = threading.Thread(target=acquire_insert_locks, args=(trans2,))

        # Start both threads
        thread1.start()
        thread2.start()

        # Wait for both threads to complete
        thread1.join()
        thread2.join()


    def test_RunOpLocks(self):
        #creates multiple transactions to test locks (they share the same table)
        trans1 = Transaction()
        trans2 = Transaction()

        def acquire_operation_locks(transaction):
            #check if successful
            res = transaction._acquire_operation_locks(self.test_table, 'b0', LockMode.EXCLUSIVE)
            #get output ids and check if what is expected
            table_id, page_range_id, page_id, record_id = transaction._get_lock_ids(self.test_table, 'b0')
            #only check if thread lock was acquired
            if res:
                self.assertEqual(transaction.held_locks[table_id], (LockGranularity.TABLE, LockMode.EXCLUSIVE), msg="table lock not held correctly")
                self.assertEqual(transaction.held_locks[page_range_id], (LockGranularity.PAGE_RANGE, LockMode.EXCLUSIVE), msg="page range lock not held correctly")
                self.assertEqual(transaction.held_locks[page_id], (LockGranularity.PAGE, LockMode.EXCLUSIVE), msg="page lock not held correctly")
                self.assertEqual(transaction.held_locks[record_id], (LockGranularity.RECORD, LockMode.EXCLUSIVE), msg="record lock not held correctly")
                #this'll fail if lock wasn't acquired
            self.assertTrue(res, msg=f"lock not acquired for thread: {threading.current_thread().name}")

        # Create two threads that try to acquire operation locks at the same time
        thread1 = threading.Thread(target=acquire_operation_locks, args=(trans1,))
        thread2 = threading.Thread(target=acquire_operation_locks, args=(trans2,))

        # Start both threads
        thread1.start()
        thread2.start()

        # Wait for both threads to complete
        thread1.join()
        thread2.join()

    def test_RunGetIds(self):
        trans1 = Transaction()
        print(f"what get_lock_ids returns: {trans1._get_lock_ids(self.test_table, 'b0')}")
        self.assertEqual(trans1._get_lock_ids(self.test_table, 'b0'), ('test',
                                                                       'test/pagerange_0',
                                                                       'test/pagerange_0/base/page_0',
                                                                       f"test/pagerange_0/base/page_0/{self.test_table.page_directory['b0'][1]}"),msg="if these tuples are different shows the differences")
    def test_run_insert(self):
        trans = Transaction()
        trans.add_query(self.query.insert, self.test_table, 3007, 9, 10, 11, 12)
        res = trans.run()
        self.assertTrue(res, msg="failed to commit and called abort returning false")
        
    def test_run_insert_dupe(self):
        trans = Transaction()
        trans.add_query(self.query.insert, self.test_table, 3005, 1, 1, 1, 1)
        res = trans.run()
        self.assertEqual(res, "duplicate_key_error", msg="duplicate key error")
        
    def test_run_select(self):
        #def select(self, search_key, search_key_index, projected_columns_index):
        '''
        self.query.insert(3005, 1, 2, 3, 4)
        self.query.insert(3006, 5, 6, 7, 8)
        query.select(key, 0, [1, 1, 1, 1, 1])[0]
        '''
        trans = Transaction()
        trans.add_query(self.query.select, self.test_table, 3006, 0, [1, 1, 1, 1, 1])
        res = trans.run()
        self.assertTrue(res)
        #self.assertEqual(self.query.select(3005, 0, [1, 1, 1, 1, 1]).columns, [3005, 1, 2, 3, 4])
    
    def test_run_update(self):
        trans = Transaction()
        trans.add_query(self.query.update, self.test_table, 3006, None, 1, None, None, None)
        res = trans.run()
        self.assertTrue(res, msg="failed to commit and called abort returning false")
        
    '''    
    def test_abort(self):
        #goes through changes list and does a query delete on each key

        pass

    def test_commit(self):
        #just releases locks after run completes
        pass
    '''



class testingTransactionWorker(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        self.db.open('./ECS165')
        self.test_table = self.db.create_table('test', 5, 0) #test name 5 col key 0
        self.query = Query(self.test_table)
        self.query.insert(3005, 2, 3, 4, 5) #rid is b0 (yes i had to write this im stupid)
    

class testing2PL(unittest.TestCase):
    def setUp(self):
        pass

#testing suites
def transactions_Suite():
    suite = unittest.TestSuite()
    
    #suite.addTest(testingTransactions('test_TransactionOBJ'))
    
    #suite.addTest(testingTransactions('test_RunInsertLocks'))
    
    #suite.addTest(testingTransactions('test_RunOpLocks'))
    #suite.addTest(testingTransactions('test_RunGetIds'))


    suite.addTest(testingTransactions('test_run_insert'))    
    suite.addTest(testingTransactions('test_run_insert_dupe'))
    suite.addTest(testingTransactions('test_run_select'))
    suite.addTest(testingTransactions('test_run_update'))
    #suite.addTest(testingTransactions('test_abort'))
    #suite.addTest(testingTransactions('test_commit'))
    
    return suite

def transactionWorker_Suite():
    suite = unittest.TestSuite()
    pass

def TwoPL_Suite():
    pass

def all_Suite():
    pass
  

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run unit tests.')
    parser.add_argument('--test', type=str, help='Specify the test to run')
    parser.add_argument('-v', '--verbose', action='store_true', help='Run tests in verbose mode')
    args = parser.parse_args()

    if args.test == 't':
        suite = transactions_Suite()
    elif args.test == 'tw':
        suite = transactionWorker_Suite()
    elif args.test == '2pl':
        suite = TwoPL_Suite()
    else:
        suite = all_Suite()

    runner = unittest.TextTestRunner(verbosity=2 if args.verbose else 1)
    runner.run(suite)
 #py test_units.py --test t 
