import threading
from collections import OrderedDict
from lstore.index import Index
from lstore.table import Table, Record
from lstore.two_phase_lock import TwoPhaseLock, LockMode, LockGranularity

class Transaction:
    """
    Unit of Concurrency Controlled Database Operations
    
    Key Features:
    1. Maintains ACID properties through Two-Phase Locking
    2. Supports different lock granularities (table/page/record)
    3. Handles both read and write operations with appropriate locks
    4. Provides atomic execution with commit/abort capabilities
    
    Lock Acquisition Strategy:
    - For INSERT:
        1. First acquire table lock
        2. Then acquire page lock
    - For other operations (UPDATE/SELECT):
        1. First acquire table lock
        2. Then acquire page lock
        3. Finally acquire record lock
    
    Transaction States:
    1. Active: During query execution
    2. Committed: All operations successful, changes permanent
    3. Aborted: Operation failed, all changes rolled back
    """
    
    # Class-level variables for transaction management
    transaction_id_counter = 0
    transaction_id_lock = threading.Lock()       # Ensures unique transaction IDs
    global_lock_manager = None                   # Shared 2PL for all transactions
    global_lock_manager_lock = threading.Lock()  # Thread-safe lock manager initialization

    @classmethod
    def get_lock_manager(cls):
        """
        Returns (or creates) the global lock manager shared by all transactions.
        Thread-safe singleton pattern implementation.
        """
        with cls.global_lock_manager_lock:
            if cls.global_lock_manager is None:
                print("\nCreating lock manager")
                cls.global_lock_manager = TwoPhaseLock()
            return cls.global_lock_manager


    def __init__(self):
        """
        Initializes a new transaction with:
        - Unique transaction ID
        - Empty query list
        - Reference to global lock manager
        - Ordered tracking of acquired locks
        """
        self.queries = []  # List of (query_function, table, args) tuples
        self.changes = []  # Track changes for rollback: (table, rid, is_insert)
        # Track locks in order of acquisition with their granularity and mode
        self.held_locks = OrderedDict()  # {item_id: (granularity, mode)}
        # Get unique transaction ID thread-safely
        with Transaction.transaction_id_lock:
            self.transaction_id = Transaction.transaction_id_counter
            Transaction.transaction_id_counter += 1
        print(f"\nCreated Transaction T{self.transaction_id}")
        self.lock_manager = Transaction.get_lock_manager()


    def _get_lock_ids(self, table, rid):
        """
        Generates hierarchical lock IDs for different granularity levels.
        
        Lock ID Format:
        - Table: "table_name"
        - Page: "table_name/page_X"
        - Record: "table_name/page_X/rid"
        
        Args:
            table: Table object to lock
            rid: Record ID for page/record locks
            
        Returns:
            Tuple of (table_lock_id, page_lock_id, record_lock_id)
        """
        table_name = getattr(table, 'name', str(id(table)))
        page_index = table.page_directory[rid][0]
        
        table_lock_id = f"{table_name}"
        page_lock_id = f"{table_name}/page_{page_index}"
        record_lock_id = f"{table_name}/page_{page_index}/{rid}"
        
        return (table_lock_id, page_lock_id, record_lock_id)


    def add_query(self, query, table, *args):
        """
        Adds a query to this transaction's execution queue.
        
        Args:
            query: Function reference to execute (e.g., update, select)
            table: Table to operate on
            *args: Arguments for the query function
        """
        print(f"\nT{self.transaction_id} adding query: {query.__name__}")
        self.queries.append((query, table, args))


    def run(self):
        """
        Executes all queries in the transaction while maintaining isolation.
        
        Execution Process:
        1. For each query:
            a. Determine lock mode (SHARED/EXCLUSIVE)
            b. Acquire necessary locks at appropriate granularity
            c. Execute the query
            d. If any step fails, abort entire transaction
        2. If all queries succeed, commit transaction
        
        Returns:
            bool: True if transaction committed, False if aborted
        """
        try:
            print(f"\nExecuting Transaction T{self.transaction_id}")
            for query, table, args in self.queries:
                print(f"\nT{self.transaction_id} executing {query.__name__}")
                
                # Determine lock type based on operation
                is_write = "update" in query.__name__ or "insert" in query.__name__
                lock_mode = LockMode.EXCLUSIVE if is_write else LockMode.SHARED
                
                # Special case: INSERT only needs table-level lock
                if "insert" in query.__name__:
                    print(f"T{self.transaction_id} requesting table lock for INSERT")
                    if not self.lock_manager.acquire_lock(
                        self.transaction_id, 
                        table.name,
                        lock_mode,
                        LockGranularity.TABLE
                    ):
                        print(f"T{self.transaction_id} failed to acquire table lock")
                        return self.abort()
                    
                    # Track acquired lock
                    self.held_locks[table.name] = (LockGranularity.TABLE, lock_mode)
                    
                    # Track insert for potential rollback
                    result = query(*args)
                    if result:  # If insert successful
                        self.changes.append((table, result, True))  # result is the RID for insert
                    else:
                        return self.abort()
                else:
                    # Get RID from args and generate lock IDs
                    rid = args[0]
                    table_id, page_id, record_id = self._get_lock_ids(table, rid)
                    
                    print(f"T{self.transaction_id} requesting locks for {query.__name__}")
                    print(f"Lock IDs: table={table_id}, page={page_id}, record={record_id}")
                    
                    # Acquire locks in hierarchical order (largest to smallest granularity)
                    if not self.lock_manager.acquire_lock(
                        self.transaction_id, table_id, lock_mode, LockGranularity.TABLE
                    ):
                        print(f"T{self.transaction_id} failed to acquire table lock")
                        return self.abort()
                    self.held_locks[table_id] = (LockGranularity.TABLE, lock_mode)
                        
                    if not self.lock_manager.acquire_lock(
                        self.transaction_id, page_id, lock_mode, LockGranularity.PAGE
                    ):
                        print(f"T{self.transaction_id} failed to acquire page lock")
                        return self.abort()
                    self.held_locks[page_id] = (LockGranularity.PAGE, lock_mode)
                        
                    if not self.lock_manager.acquire_lock(
                        self.transaction_id, record_id, lock_mode, LockGranularity.RECORD
                    ):
                        print(f"T{self.transaction_id} failed to acquire record lock")
                        return self.abort()
                    self.held_locks[record_id] = (LockGranularity.RECORD, lock_mode)

                    # Track update for potential rollback
                    if "update" in query.__name__:
                        self.changes.append((table, rid, False))  # False indicates update
                    
                    # Execute query after acquiring all necessary locks
                    result = query(*args)
                    if not result:
                        print(f"T{self.transaction_id} query failed")
                        return self.abort()

            print(f"T{self.transaction_id} all queries successful")
            return self.commit()
        except Exception as e:
            print(f"T{self.transaction_id} failed with error: {str(e)}")
            return self.abort()


    def abort(self):
        """
        Aborts the transaction and rolls back any changes.
        
        Rollback Process:
        1. Mark all changes as deleted in reverse order:
           - For inserts: Delete the record using primary key
           - For updates: Delete the record using primary key
        2. Release all locks in reverse order of acquisition (automatically handles granularity order)
        3. Return False to indicate transaction failure
        """
        print(f"\nAborting Transaction T{self.transaction_id}")
        
        # Rollback changes in reverse order
        print(f"Rolling back changes for T{self.transaction_id}")
        for table, rid, is_insert in reversed(self.changes):
            print(f"Deleting {'inserted' if is_insert else 'updated'} record {rid}")
            # Get primary key from the record to delete it
            primary_key = table.index.get_key(0, rid)  # 0 is primary key index
            if primary_key is not None:
                from lstore.query import Query
                query = Query(table)
                query.delete(primary_key)
        
        # Release all locks in reverse order of acquisition
        # This automatically handles granularity order since we acquired in correct order
        if self.lock_manager:
            for item_id in reversed(self.held_locks):
                self.lock_manager.release_lock(self.transaction_id, item_id)
            self.held_locks.clear()
        
        print(f"T{self.transaction_id} abort complete")
        return False


    def commit(self):
        """
        Commits the transaction, making all changes permanent.
        
        Commit Process:
        1. Ensure all changes are written to storage
        2. Release all locks in reverse order of acquisition (automatically handles granularity order)
        3. Return True to indicate transaction success
        """
        print(f"\nCommitting Transaction T{self.transaction_id}")
        
        # Release all locks in reverse order of acquisition
        # This automatically handles granularity order since we acquired in correct order
        if self.lock_manager:
            for item_id in reversed(self.held_locks):
                self.lock_manager.release_lock(self.transaction_id, item_id)
            self.held_locks.clear()
        
        print(f"T{self.transaction_id} commit complete")
        return True