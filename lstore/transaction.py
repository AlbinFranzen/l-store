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
    transaction_id_counter = 0                   # counts transaction ids
    transaction_id_lock = threading.Lock()       # Ensures unique transaction IDs
    global_lock_manager = None                   # Static/Shared 2PL for all transactions
    global_lock_manager_lock = threading.Lock()  # Thread-safe lock manager initialization

    @classmethod
    def get_lock_manager(cls):
        """
        Returns (or creates) the global lock manager shared by all transactions.
        """
        with cls.global_lock_manager_lock:
            if cls.global_lock_manager is None:
                #print("\nCreating lock manager")
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
        #print(f"\nCreated Transaction T{self.transaction_id}")
        self.lock_manager = Transaction.get_lock_manager()


    def _get_lock_ids(self, table, rid):
        """
        Generates hierarchical lock IDs for different granularity levels.

        Lock ID Format:
        - Table: "table_name"
        - Page Range: "table_name/pagerange_X"
        - Page: "table_name/pagerange_X/[base|tail]/page_Y"
        - Record: "table_name/pagerange_X/[base|tail]/page_Y/rid"

        Args:
            table: Table object to lock
            rid: Record ID for page/record locks

        Returns:
            Tuple of (table_lock_id, page_range_id, page_lock_id, record_lock_id)
        """
        table_name = getattr(table, 'name', str(id(table)))
        page_path = table.page_directory[rid][0]
        record_offset = table.page_directory[rid][1]

        # Split path into components (e.g., [".", database, _tables, tablename, pagerange_0", "base", "page_1"])
        parts = page_path.split('/')
        #print("parts: ",  parts)

        #ecs165/_tables/tablename/pagegrange/base/page
        # Extract page range number from path (e.g., "pagerange_0" -> "0")
        page_range = parts[4]
        # Get page type (base or tail) and page number
        page_type = parts[5]  # "base" or "tail"
        page_num = parts[6]   # "page_X"

        # Generate lock IDs for each granularity level
        table_lock_id = f"{table_name}"
        page_range_id = f"{table_name}/{page_range}"
        page_lock_id = f"{table_name}/{page_range}/{page_type}/{page_num}"
        record_lock_id = f"{table_name}/{page_range}/{page_type}/{page_num}/{record_offset}"

        return table_lock_id, page_range_id, page_lock_id, record_lock_id


    def add_query(self, query, table, *args):
        """
        Adds a query to this transaction's execution queue.

        Args:
            query: Function reference to execute (e.g., update, select)
            table: Table to operate on
            *args: Arguments for the query function
        """
        #print(f"\nT{self.transaction_id} adding query: {query.__name__}")
        self.queries.append((query, table, args))


    def run(self):
        """
        Executes all queries in the transaction while maintaining isolation.
        Returns True if all operations succeed, False otherwise.
        """
        #print(f"\nExecuting Transaction T{self.transaction_id}")
        # If any query in this transaction is an insert or update, force exclusive locks
        overall_exclusive = any("update" in q.__name__ or "insert" in q.__name__ for q, table, args in self.queries)

        try:
            for query, table, args in self.queries:
                #print(f"\nT{self.transaction_id} executing {query.__name__} with args: {args}")
                # Decide lock_mode: if overall_exclusive is True then use EXCLUSIVE for every operation
                if overall_exclusive or ("update" in query.__name__ or "insert" in query.__name__):
                    lock_mode = LockMode.EXCLUSIVE
                else:
                    lock_mode = LockMode.SHARED

                if "insert" in query.__name__:
                    if not self._acquire_insert_locks(table, lock_mode):
                        #print(f"T{self.transaction_id} failed to acquire locks for insert")
                        return self.abort()
                else:
                    rid = table.index.locate(0, args[0])
                    if not rid:
                        #print(f"T{self.transaction_id} could not locate record with key {args[0]}")
                        return self.abort()
                    if not self._acquire_operation_locks(table, rid, lock_mode):
                        #print(f"T{self.transaction_id} failed to acquire operation locks")
                        return self.abort()

                # Execute the query
                result = query(*args)

                # Handle query result
                if result is False:
                    if "insert" in query.__name__:
                        return self.abort(dupe_error=True)
                    return self.abort()

                # Track successful operations for potential rollback
                if "insert" in query.__name__:
                    #print(f"T{self.transaction_id} successfully inserted record with key {args[0]}")
                    self.changes.append((table, args[0], True))
                elif "update" in query.__name__:
                    #print(f"T{self.transaction_id} successfully updated record with key {args[0]}")
                    self.changes.append((table, args[0], False))

            #print(f"T{self.transaction_id} all queries successful")
            return self.commit(), None

        except Exception as e:
            #print(f"T{self.transaction_id} failed with error: {str(e)}")
            import traceback
            traceback.print_exc()
            return self.abort()


    def _acquire_insert_locks(self, table, lock_mode):
        """
        Helper method to acquire locks for insert operations
        Inserts require table lock
        """
        #print(f"T{self.transaction_id} requesting table lock for INSERT")
        # Acquire table lock from lock manager
        if not self.lock_manager.acquire_lock(
                self.transaction_id,
                table.name,
                lock_mode,
                LockGranularity.TABLE
        ):
            #print(f"T{self.transaction_id} failed to acquire table lock")
            return False
        # {item_id: (granularity, mode)}
        self.held_locks[table.name] = (LockGranularity.TABLE, lock_mode)
        return True


    def _acquire_operation_locks(self, table, rid, lock_mode):
        """Helper method to acquire hierarchical locks for other operations"""
        table_id, page_range_id, page_id, record_id = self._get_lock_ids(table, rid)
        #print(f"T{self.transaction_id} requesting locks for operation")
        #print(f"Lock IDs: table={table_id}, page_range={page_range_id}, page={page_id}, record={record_id}")

        # Define lock hierarchy
        locks_to_acquire = [
            (table_id, LockGranularity.TABLE),
            (page_range_id, LockGranularity.PAGE_RANGE),
            (page_id, LockGranularity.PAGE),
            (record_id, LockGranularity.RECORD)
        ]

        # Acquire locks in hierarchical order
        for item_id, granularity in locks_to_acquire:
            if not self.lock_manager.acquire_lock(
                    self.transaction_id, item_id, lock_mode, granularity
            ):
                #print(f"T{self.transaction_id} failed to acquire {granularity} lock")
                return False
            self.held_locks[item_id] = (granularity, lock_mode)

        return True


    def abort(self, dupe_error=False):
        """
        Aborts the transaction and rolls back any changes.
        """
        #print(f"\nAborting Transaction T{self.transaction_id}")

        try:
            # Rollback changes in reverse order
            #print(f"Rolling back changes for T{self.transaction_id}")
            for table, key, is_insert in reversed(self.changes):
                try:
                    #print(f"Rolling back {'insert' if is_insert else 'update'} for key {key}")
                    # For inserts, delete the record
                    from lstore.query import Query
                    query = Query(table)
                    if not query.delete(key):
                        pass
                        #print(f"Warning: Failed to rollback insert for key {key}")
                except Exception as e:
                    #print(f"Error during rollback: {str(e)}")
                    continue
        finally:
            # Always release locks, even if rollback fails
            if self.lock_manager:
                #print(f"Releasing locks for T{self.transaction_id}")
                for item_id in reversed(self.held_locks):
                    self.lock_manager.release_lock(self.transaction_id, item_id)
                self.held_locks.clear()

        #print(f"T{self.transaction_id} abort complete")
        
        return False, "dupe_error" if dupe_error else None


    def commit(self):
        """
        Commits the transaction, making all changes permanent.

        Commit Process:
        1. Ensure all changes are written to storage
        2. Release all locks in reverse order of acquisition (automatically handles granularity order)
        3. Return True to indicate transaction success
        """
        #print(f"\nCommitting Transaction T{self.transaction_id}")

        # Release all locks in reverse order of acquisition
        # This automatically handles granularity order since we acquired in correct order
        if self.lock_manager:
            for item_id in reversed(self.held_locks):
                self.lock_manager.release_lock(self.transaction_id, item_id)
            self.held_locks.clear()

        #print(f"T{self.transaction_id} commit complete")
        return True
