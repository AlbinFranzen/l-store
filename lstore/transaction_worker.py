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
    4. Call join() to wait for completion and get results
    """

    def __init__(self, transactions=None):
        """
        Initialize transaction worker.
        
        Args:
            transactions: Optional list of transactions to execute
            
        State Management:
            stats: List tracking success/failure of each transaction
            result: Count of successful transactions
            thread: Thread for executing transactions
            _stats_lock: Ensures thread-safe stats updates
        """
        if transactions is None:
            transactions = []
        self.stats = []                             # Track success/failure of each transaction
        self.transactions = transactions.copy()     # Make a copy to avoid external modifications
        self.result = 0                             # Number of successful transactions
        self.thread = None                          # Thread to execute transactions
        self._stats_lock = threading.Lock()         # Lock for thread-safe stats updates


    def add_transaction(self, transaction):
        """
        Adds a transaction to the execution queue.
        Must be called before run() is invoked.
        
        Args:
            transaction: Transaction object to execute
        """
        self.transactions.append(transaction)


    def run(self):
        """
        Starts asynchronous execution of all transactions.
        Creates a new thread to run transactions concurrently.
        """
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()


    def join(self):
        """
        Waits for all transactions to complete.
        
        Returns:
            int: Number of successfully completed transactions
        """
        if self.thread:
            self.thread.join()
        return self.result


    def __run(self):
        """
        Internal method to execute transactions.
        
        Execution Process:
        1. For each transaction:
            a. Attempt to run the transaction
            b. Track success/failure status
            c. Handle any exceptions
        2. Calculate final success count
        
        Thread Safety:
        - Uses _stats_lock to safely update shared state
        - Copies transaction list to avoid external modifications
        - Handles exceptions to prevent thread crashes
        """
        for transaction in self.transactions:
            try:
                success = transaction.run()
                with self._stats_lock:
                    self.stats.append(success)
            except Exception as e:
                print(f"Transaction failed with error: {str(e)}")
                with self._stats_lock:
                    self.stats.append(False)
        
        # Calculate final result (count of successful transactions)
        with self._stats_lock:
            self.result = sum(1 for stat in self.stats if stat)