import time
import os
import sys
import threading
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(sys.path)
from lstore.two_phase_lock import LockMode, LockGranularity, TwoPhaseLock

# Lock manager for two-phase locking
lock_manager = TwoPhaseLock()


def transaction_task_record_level(transaction_id):
    print(f"[Transaction {transaction_id}] Starting transaction_task_record_level")

    table_id = "Table"
    record_id = "Table/pagerange_0/base/page_0/offset_1"  

    # 1) Acquire table-level SHARED lock
    acquired_table_s = lock_manager.acquire_lock(
        transaction_id, table_id, LockMode.SHARED, LockGranularity.TABLE
    )

    if not acquired_table_s:
        print(f"[Transaction {transaction_id}] Could NOT acquire SHARED lock on {table_id}, aborting.")
        return
    print(f"[Transaction {transaction_id}] Acquired SHARED lock on {table_id}, simulating read...")
    time.sleep(1)  # simulate reading data

    # 2) Acquire record-level EXCLUSIVE lock
    acquired_record_x = lock_manager.acquire_lock(
        transaction_id, record_id, LockMode.EXCLUSIVE, LockGranularity.RECORD
    )

    if not acquired_record_x:
        print(f"[Transaction {transaction_id}] Could NOT acquire EXCLUSIVE lock on {record_id}, rolling back and releasing table lock")
        lock_manager.release_lock(transaction_id, table_id)
        return

    print(f"[Transaction {transaction_id}] Acquired EXCLUSIVE lock on {record_id}, simulating write...")
    time.sleep(2)  # simulate write operation

    # release record lock
    lock_manager.release_lock(transaction_id, record_id)
    print(f"[Transaction {transaction_id}] Released EXCLUSIVE lock on {record_id}")

    # release table lock
    lock_manager.release_lock(transaction_id, table_id)
    print(f"[Transaction {transaction_id}] Released SHARED lock on table {table_id}, transaction complete")


def transaction_task_multi_records(transaction_id):
    print(f"[Transaction {transaction_id}] Starting transaction_task_multi_records")

    recordA = "Table_Users/pagerange_0/base/page_1/offset_5"
    recordB = "Table_Users/pagerange_0/base/page_2/offset_10"

    # Acquire recordA EXCLUSIVE lock
    acquiredA = lock_manager.acquire_lock(
        transaction_id, recordA, LockMode.EXCLUSIVE, LockGranularity.RECORD
    )

    if not acquiredA:
        print(f"[Transaction {transaction_id}] Could NOT acquire EXCLUSIVE lock on recordA, aborting.")
        return
    print(f"[Transaction {transaction_id}] Locked recordA, simulating partial write...")
    time.sleep(1)

    # Acquire recordB EXCLUSIVE lock
    acquiredB = lock_manager.acquire_lock(
        transaction_id, recordB, LockMode.EXCLUSIVE, LockGranularity.RECORD
    )

    if not acquiredB:
        print(f"[Transaction {transaction_id}] Could NOT acquire EXCLUSIVE lock on recordB, rolling back and releasing recordA.")
        lock_manager.release_lock(transaction_id, recordA)
        return
    print(f"[Transaction {transaction_id}] Locked recordB, simulating write on both records...")
    time.sleep(2)

    # release recordB
    lock_manager.release_lock(transaction_id, recordB)
    print(f"[Transaction {transaction_id}] Released EXCLUSIVE lock on recordB")

    # release recordA
    lock_manager.release_lock(transaction_id, recordA)
    print(f"[Transaction {transaction_id}] Released EXCLUSIVE lock on recordA, transaction done")


def transaction_task_table_exclusive(transaction_id):
    table_name = "Table_Orders"
    print(f"[Transaction {transaction_id}] Attempting to acquire EXCLUSIVE lock on {table_name}...")
    acquired = lock_manager.acquire_lock(
        transaction_id, table_name, LockMode.EXCLUSIVE, LockGranularity.TABLE
    )
    
    if not acquired:
        print(f"[Transaction {transaction_id}] Could NOT acquire table-level EXCLUSIVE lock, aborting.")
        return
    print(f"[Transaction {transaction_id}] Acquired EXCLUSIVE lock on {table_name}, simulating write...")
    time.sleep(3)

    # release lock
    lock_manager.release_lock(transaction_id, table_name)
    print(f"[Transaction {transaction_id}] Released EXCLUSIVE lock on {table_name}, transaction complete.")


def run_extended_test():
    print("\n=== Test Begin ===\n")

    t1 = threading.Thread(target=transaction_task_record_level, args=(1,))
    t2 = threading.Thread(target=transaction_task_table_exclusive, args=(2,))
    t3 = threading.Thread(target=transaction_task_multi_records, args=(3,))

    t1.start()
    time.sleep(0.2)  # let T1 acquire table-level SHARED first
    t2.start()
    time.sleep(0.2)  # start T2
    t3.start()       # T3 deals with a different table

    t1.join()
    t2.join()
    t3.join()

    print("\n=== Test Finished ===\n")


if __name__ == "__main__":
    run_extended_test()
