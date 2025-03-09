import threading
from lstore.index import Index
from lstore.table import Table, Record

class TransactionWorker:
    """
    Manages concurrent execution of multiple transactions.

    Key Features:
    1. Thread-safe transaction execution
    2. Tracks success/failure of transactions
    3. Provides aggregated results
    4. Handles transaction isolation

    Usage:
    1. Create worker with list of transactions
    2. Add additional transactions if needed
    3. Call run() to execute transactions in separate thread
    4. Call join() to wait for completion
    """
    
    txn_worker_id_counter = 0                   # counts transaction ids
    txn_worker_id_lock = threading.Lock()       # Ensures unique transaction IDs

    def __init__(self, transactions=None):
        """
        Args:
            transactions: Optional list of transactions to execute

        State Management:
            thread: Thread for executing transactions
        """
        if transactions is None:
            transactions = []
        self.transactions = transactions.copy()     # Make a copy to avoid external modifications
        self.thread = None                          # Thread to execute transactions
        
        with TransactionWorker.txn_worker_id_lock:
            self.worker_id = TransactionWorker.txn_worker_id_counter
            TransactionWorker.txn_worker_id_counter += 1


    def add_transaction(self, transaction):
        """
        Adds a transaction to the execution queue.
        Must be called before run() is invoked.

        Args:
            transaction: Transaction object to execute
        """
        print(f"Adding T{transaction.transaction_id} to worker {self.worker_id} ")
        self.transactions.append(transaction)


    def run(self):
        """
        Starts asynchronous execution of all transactions.
        Creates a new thread to run transactions concurrently.
        """
        print(f"\nStarting worker {self.worker_id} with {len(self.transactions)} transactions")
        self.thread = threading.Thread(target=self._run)
        self.thread.start()


    def join(self):
        """
        Waits for all transactions to complete.

        Returns:
            int: Number of successfully completed transactions
        """
        if self.thread:
            print(f"Waiting for worker {self.worker_id} thread to complete...")
            self.thread.join()
            print(f"Worker {self.worker_id} thread completed")
        pass

    def _run(self):
        """
        Execution Process:
        1. For each transaction:
            a. Attempt to run the transaction
            b. Track success/failure status
            c. Handle any exceptions
        2. Calculate final success count

        Thread Safety:
        - Copies transaction list to avoid external modifications
        - Handles exceptions to prevent thread crashes
        """
        for transaction in self.transactions:
            #print(f"\nWorker {self.worker_id} processing T{transaction.transaction_id}")
            try:
                result, dupe = transaction.run()
                while result is not True:
                    #print(f"T{transaction.transaction_id} failed or was aborted")
                    if dupe == "dupe_error":
                        print("dupe_error, skipping transaction...")
                        break
                    result, dupe = transaction.run()
                    
                print("Result: ", result)
                if dupe == "dupe_error":
                    print("dupe_error, skipping transaction...")
                    continue
                else:
                    print(f"T{transaction.transaction_id} completed successfully")

            except Exception as e:
                print(f"T{transaction.transaction_id} failed with error: {e}")
                import traceback
                traceback.print_exc()

