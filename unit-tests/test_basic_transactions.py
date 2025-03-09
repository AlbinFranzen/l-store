import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
from lstore.two_phase_lock import TwoPhaseLock, LockMode, LockGranularity

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')

grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)


try:
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not implemented properly, tests may fail.')

seed(3562901)



# UNIT TESTS

# checks whether a query is being added correctly to the query list
def test_add_query():
    t = Transaction()
    t.add_query(query.insert, grades_table, 99999, 80, 90, 85, 88)
    if len(t.queries) == 1:
        print("add query passed")
    else:
        print("add query failed")

# checks whether lock ids are correctly identified for a given record
def test_get_lock_ids():
    table = grades_table
    table.page_directory = {99999: ("pagerange_0/base/page_1", 5)}

    t = Transaction()
    lock_ids = t._get_lock_ids(table, 99999)

    expected_locks = (
        "Grades",
        "Grades/pagerange_0",
        "Grades/pagerange_0/base/page_1",
        "Grades/pagerange_0/base/page_1/5"
    )

    if lock_ids == expected_locks:
        print("get_lock_ids passed")
    else:
        print(f"_get_lock_ids failed: {lock_ids}")

# checks whether acquiring an exclusive lock on a table while inserting is successful
def test_acquire_insert_locks():
    t = Transaction()
    result = t._acquire_insert_locks(grades_table, LockMode.EXCLUSIVE)

    if result is True:
        if grades_table.name in t.held_locks:
            print("_acquire_insert_locks passed")
        else:
            print("lock not acquired")
    else:
        print("_acquire_insert_locks failed")

# checks whether a shared lock can be acquired when an exclusive lock already exists 
def test_acquire_operation_locks():
    t1 = Transaction()
    t1._acquire_insert_locks(grades_table, LockMode.EXCLUSIVE)

    t2 = Transaction()
    result = t2._acquire_operation_locks(grades_table, 99999, LockMode.SHARED)

    if result is False:
        print("_acquire_operation_locks passed")
    else:
        print("_acquire_operation_locks failed when an exclusive lock exists")

# checks whether abort is able to rollback and release all locks successfully
def test_abort():
    t = Transaction()
    t.held_locks = {
        "Grades": (LockGranularity.TABLE, LockMode.EXCLUSIVE),
        "Grades/pagerange_0": (LockGranularity.PAGE_RANGE, LockMode.EXCLUSIVE),
    }
    result = t.abort()
    if result is False:
        if len(t.held_locks) == 0:
            print("abort passed")
        else:
            print("abort failed")
    else:
        print("locks were not released after abort")
    

# checks if all changes were committed and locks were released
def test_commit():
    t = Transaction()
    t.held_locks = {
        "Grades": (LockGranularity.TABLE, LockMode.EXCLUSIVE),
        "Grades/pagerange_0": (LockGranularity.PAGE_RANGE, LockMode.EXCLUSIVE),
    }
    result = t.commit()
    if result is True:
        if len(t.held_locks) == 0:
            print("commit passed")
        else:
            print("locks were not released after commit")
    else:
        print("commit failed")

# should return True if insert is successful
def test_run_success():
    db.close()
    db.open('./ECS165')

    t = Transaction()
    t.add_query(query.insert, grades_table, 99999, 95, 90, 80, 85)
    result = t.run()
    print(f"Transaction result: {result}")  # Debugging
    if result is True:
        print("run_success passed")
    else:
        print("run_success failed")  
    
# checks whether transaction correctly aborts when there is an error with a query
def test_run_fail():
    grades_table.page_directory = {123: ("pagerange_0/base/page_1", 5)}


    def failing_query(*args):
        raise KeyError("simulated error")
    
    t = Transaction()
    t.add_query(failing_query, grades_table, 123)

    try:
        result = t.run()
    except KeyError:
        print("Expected key error occurred")
        result = False
    if result is False:
        print("run_fail passed")
    else:
        print("run_fail did not abort correctly")



test_add_query()
test_get_lock_ids()
test_acquire_insert_locks()
test_acquire_operation_locks()
test_abort()
test_commit()
test_run_success()
test_run_fail()

db.close()