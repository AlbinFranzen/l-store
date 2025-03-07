import threading

class LockMode:
    """
    Defines the types of locks that can be acquired:
    - SHARED (0): Multiple transactions can read the same data
    - EXCLUSIVE (1): Only one transaction can write to the data
    """
    SHARED, EXCLUSIVE = enumerate([1,2])

    @staticmethod
    def to_string(mode):
        return "SHARED" if mode == LockMode.SHARED else "EXCLUSIVE"



class LockGranularity:
    """
    Defines the levels at which locks can be acquired:
    - TABLE (0): Locks entire table
    - PAGE (1): Locks a specific page in the table
    - RECORD (2): Locks an individual record
    
    Locks must be acquired in hierarchical order (table -> page -> record)
    """
    TABLE, PAGE, RECORD = enumerate([1,2,3])

    @staticmethod
    def to_string(granularity):
        if granularity == LockGranularity.TABLE:
            return "TABLE"
        elif granularity == LockGranularity.PAGE:
            return "PAGE"
        return "RECORD"



class Transaction:
    """
    Internal transaction class used by TwoPhaseLock to track transaction state.
    Only tracks if transaction has entered shrinking phase (when it starts releasing locks).
    
    Two-Phase Locking Protocol:
    1. Growing Phase: Transaction can acquire locks but cannot release any
    2. Shrinking Phase: Transaction can release locks but cannot acquire new ones
    """
    def __init__(self, transaction_id):
        self.transaction_id = transaction_id
        self.shrinking_phase = False  # Once True, transaction cannot acquire new locks



class TwoPhaseLock:
    """
    Implements Two-Phase Locking protocol with multiple granularity levels.
    
    Lock State Management:
    - Each resource (table/page/record) has readers (shared) and one writer (exclusive)
    - Lock state is tracked in separate dictionaries for each granularity level
    - Each lock entry contains:
        - readers: set of transaction IDs that have shared locks
        - writer: transaction ID that has exclusive lock (if any)
    
    Lock Compatibility Rules:
    1. Multiple transactions can hold shared locks simultaneously
    2. Only one transaction can hold an exclusive lock
    3. Cannot get exclusive lock if any shared locks exist
    4. Cannot get shared lock if exclusive lock exists
    5. A transaction can upgrade its shared lock to exclusive if no other readers
    """
    def __init__(self):
        self.mut = threading.Lock()  # Mutex for thread-safe lock operations
        self.transactions = {}       # Maps transaction_id to Transaction object
        
        # Lock dictionaries for each granularity level
        # Format: {item_id: {"readers": set(), "writer": None}}
        self.table_locks = {}  # Locks on entire tables
        self.page_locks = {}   # Locks on specific pages
        self.record_locks = {} # Locks on individual records


    def _get_lock_dict(self, granularity: LockGranularity):
        """
        Returns the appropriate lock dictionary for the given granularity level.
        This allows uniform lock handling regardless of granularity.
        """
        if granularity == LockGranularity.TABLE:
            return self.table_locks
        elif granularity == LockGranularity.PAGE:
            return self.page_locks
        else:  # RECORD
            return self.record_locks


    def _has_lock(self, transaction_id: int, item_id: str, lock_dict: dict) -> bool:
        """
        Checks if a transaction already holds any type of lock (shared or exclusive) on an item.
        
        Args:
            transaction_id: ID of transaction to check
            item_id: Resource identifier to check
            lock_dict: Dictionary containing lock information
            
        Returns:
            True if transaction holds either type of lock, False otherwise
        """
        if item_id not in lock_dict:
            return False
        lock_info = lock_dict[item_id]
        return transaction_id in lock_info["readers"] or lock_info["writer"] == transaction_id


    def _check_parent_locks(self, transaction_id: int, item_id: str, mode: LockMode, granularity: LockGranularity) -> bool:
        """
        Checks for conflicts with parent resource locks.
        Implements hierarchical locking rules:
        - To lock a record, must check table and page locks
        - To lock a page, must check table locks
        - To lock a table, no parent checks needed
        
        Args:
            transaction_id: ID of transaction requesting lock
            item_id: Resource identifier (e.g., "table/page_1/rid_2")
            mode: Type of lock requested (SHARED/EXCLUSIVE)
            granularity: Level of lock requested (TABLE/PAGE/RECORD)
            
        Returns:
            True if no conflicts with parent locks, False if conflicts exist
        """
        parts = item_id.split('/')
        print(f"\nChecking parent locks for T{transaction_id} requesting {LockMode.to_string(mode)} "
              f"lock on {LockGranularity.to_string(granularity)} {item_id}")
        
        if granularity == LockGranularity.RECORD:
            # For record locks, check both table and page
            table_id = parts[0]
            page_id = f"{parts[0]}/{parts[1]}"
            
            # Check table lock conflicts
            if (table_id in self.table_locks and 
                (self.table_locks[table_id]["writer"] is not None and 
                 self.table_locks[table_id]["writer"] != transaction_id)):
                print(f"DENIED: Table {table_id} is exclusively locked by T{self.table_locks[table_id]['writer']}")
                return False
                
            # Check page lock conflicts
            if (page_id in self.page_locks and 
                (self.page_locks[page_id]["writer"] is not None and 
                 self.page_locks[page_id]["writer"] != transaction_id)):
                print(f"DENIED: Page {page_id} is exclusively locked by T{self.page_locks[page_id]['writer']}")
                return False
                
        elif granularity == LockGranularity.PAGE:
            # For page locks, only check table
            table_id = parts[0]
            if (table_id in self.table_locks and 
                (self.table_locks[table_id]["writer"] is not None and 
                 self.table_locks[table_id]["writer"] != transaction_id)):
                print(f"DENIED: Table {table_id} is exclusively locked by T{self.table_locks[table_id]['writer']}")
                return False
        
        print(f"GRANTED: No conflicting parent locks found")
        return True


    def acquire_lock(self, transaction_id: int, item_id: str, mode: LockMode, granularity: LockGranularity) -> bool:
        """
        Attempts to acquire a lock for a transaction following 2PL rules.
        
        Lock Acquisition Process:
        1. Check if transaction is in shrinking phase
        2. Check if transaction already has the requested lock
        3. Check for conflicts with parent resource locks
        4. Check lock compatibility with existing locks
        5. Grant lock if all checks pass
        
        Args:
            transaction_id: ID of transaction requesting lock
            item_id: Resource identifier (e.g., "table/page_1/rid_2")
            mode: Type of lock requested (SHARED/EXCLUSIVE)
            granularity: Level of lock requested (TABLE/PAGE/RECORD)
            
        Returns:
            True if lock was acquired, False if denied
        """
        with self.mut:
            print(f"\nT{transaction_id} requesting {LockMode.to_string(mode)} lock on "
                  f"{LockGranularity.to_string(granularity)} {item_id}")
            
            # Create transaction object if not exists
            if transaction_id not in self.transactions:
                self.transactions[transaction_id] = Transaction(transaction_id)
            
            transaction = self.transactions[transaction_id]
            
            # Cannot acquire new locks in shrinking phase (2PL rule)
            if transaction.shrinking_phase:
                print(f"DENIED: T{transaction_id} is in shrinking phase")
                return False

            # Get appropriate lock dictionary for this granularity
            lock_dict = self._get_lock_dict(granularity)
            
            # Check if transaction already has this lock
            if self._has_lock(transaction_id, item_id, lock_dict):
                print(f"T{transaction_id} already has lock on {item_id}")
                return True

            # Check parent locks (hierarchical locking)
            if not self._check_parent_locks(transaction_id, item_id, mode, granularity):
                return False

            # Initialize lock info if this is first lock on item
            if item_id not in lock_dict:
                lock_dict[item_id] = {"readers": set(), "writer": None}
            
            lock_info = lock_dict[item_id]
            
            # Print current lock state for debugging
            print(f"Current lock state for {item_id}:")
            print(f"  - Readers: {lock_info['readers']}")
            print(f"  - Writer: T{lock_info['writer']} if lock_info['writer'] is not None else 'None'")
            
            # Handle shared lock request
            if mode == LockMode.SHARED:
                if lock_info["writer"] is None or lock_info["writer"] == transaction_id:
                    lock_info["readers"].add(transaction_id)
                    print(f"GRANTED: T{transaction_id} acquired SHARED lock")
                    return True
                print(f"DENIED: Item is exclusively locked by T{lock_info['writer']}")
            
            # Handle exclusive lock request
            else:  # EXCLUSIVE
                if (not lock_info["readers"] or lock_info["readers"] == {transaction_id}) and \
                   (lock_info["writer"] is None or lock_info["writer"] == transaction_id):
                    lock_info["writer"] = transaction_id
                    print(f"GRANTED: T{transaction_id} acquired EXCLUSIVE lock")
                    return True
                print(f"DENIED: Conflicting readers {lock_info['readers']} or writer T{lock_info['writer']}")
            
            return False


    def release_lock(self, transaction_id: int, item_id: str):
        """
        Releases locks held by a transaction on a specific item.
        
        Release Process:
        1. Mark transaction as in shrinking phase
        2. Release both shared and exclusive locks if held
        3. Remove locks at all granularity levels
        
        Args:
            transaction_id: ID of transaction releasing locks
            item_id: Resource identifier to release locks from
        """
        with self.mut:
            print(f"\nT{transaction_id} releasing locks on {item_id}")
            
            if transaction_id not in self.transactions:
                print(f"No transaction found for T{transaction_id}")
                return
            
            # Mark transaction as in shrinking phase (2PL rule)
            transaction = self.transactions[transaction_id]
            transaction.shrinking_phase = True
            
            # Release locks at all granularity levels
            for granularity in [LockGranularity.RECORD, LockGranularity.PAGE, LockGranularity.TABLE]:
                lock_dict = self._get_lock_dict(granularity)
                if item_id in lock_dict:
                    lock_info = lock_dict[item_id]
                    
                    # Release shared lock if held
                    if transaction_id in lock_info["readers"]:
                        lock_info["readers"].remove(transaction_id)
                        print(f"Released shared lock for T{transaction_id} on {LockGranularity.to_string(granularity)} {item_id}")
                    
                    # Release exclusive lock if held
                    if lock_info["writer"] == transaction_id:
                        lock_info["writer"] = None
                        print(f"Released exclusive lock for T{transaction_id} on {LockGranularity.to_string(granularity)} {item_id}")