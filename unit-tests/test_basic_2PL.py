import threading
import time
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lstore.two_phase_lock import TwoPhaseLock, LockMode, LockGranularity

# Lock manager for two-phase locking
lock_manager = TwoPhaseLock()

def transaction_task_A(transaction_id):
    """
    Transaction A:
    1) Acquires an exclusive lock on resource1
    2) Waits for a few seconds to simulate some work
    """
    print(f"\n[Transaction {transaction_id}] Starting transaction A...")

    # Try to acquire an exclusive lock
    resource1 = "table_Users"
    acquired = lock_manager.acquire_lock(transaction_id, resource1, LockMode.EXCLUSIVE, LockGranularity.TABLE)
    if not acquired:
        print(f"[Transaction {transaction_id}] Could not acquire EXCLUSIVE lock on {resource1}, aborting.")
        return

    print(f"[Transaction {transaction_id}] EXCLUSIVE lock acquired on {resource1}, doing some work...")
    time.sleep(2)  # Simulate some write operation

    # Release the lock
    lock_manager.release_lock(transaction_id, resource1)
    print(f"[Transaction {transaction_id}] Transaction A completed, lock released.")

def transaction_task_B(transaction_id):
    """
    Transaction B:
    1) Acquires a shared lock on resource1
    2) Waits for a few seconds to simulate some work
    """
    print(f"\n[Transaction {transaction_id}] Starting transaction B...")

    # Try to acquire a shared lock
    resource1 = "table_Users"
    acquired = lock_manager.acquire_lock(transaction_id, resource1, LockMode.SHARED, LockGranularity.TABLE)
    if not acquired:
        print(f"[Transaction {transaction_id}] Could not acquire SHARED lock on {resource1}, aborting.")
        return

    print(f"[Transaction {transaction_id}] SHARED lock acquired on {resource1}, doing some read-only work...")
    time.sleep(2)  


    lock_manager.release_lock(transaction_id, resource1)
    print(f"[Transaction {transaction_id}] Transaction B completed, lock released.")


def test_two_phase_lock():
    """
    Demonstrates a simple test case for two-phase locking
    Two transactions are run concurrently, one acquiring an exclusive lock and the other acquiring a shared lock
    """
    print("\n=== DEMO: Two-Phase Locking Test ===")


    t1 = threading.Thread(target=transaction_task_A, args=(1,))
    t2 = threading.Thread(target=transaction_task_B, args=(2,))

    t1.start()
    time.sleep(0.5)  # wait for a bit before starting the next transaction
    t2.start()

    t1.join()
    t2.join()

    print("\n=== Test Finished ===")

if __name__ == "__main__":
    test_two_phase_lock()

