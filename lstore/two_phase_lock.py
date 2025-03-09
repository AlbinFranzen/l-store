import threading

class LockMode:
    """
    - SHARED (0): Multiple transactions can read the same data
    - EXCLUSIVE (1): Only one transaction can write to the data
    """
    SHARED, EXCLUSIVE = 0, 1  # Fixing the usage of enumerate

    @staticmethod
    def to_string(mode):
        return "SHARED" if mode == LockMode.SHARED else "EXCLUSIVE"


class LockGranularity:
    """
    - TABLE (0): Locks entire table
    - PAGE_RANGE (1): Locks a specific page range in the table
    - PAGE (2): Locks a specific page in the table
    - RECORD (3): Locks an individual record
    Locks must be acquired in hierarchical order (table -> page_range -> page -> record)
    """
    TABLE, PAGE_RANGE, PAGE, RECORD = 0, 1, 2, 3  # Fixing the usage of enumerate

    @staticmethod
    def to_string(granularity):
        if granularity == LockGranularity.TABLE:
            return "TABLE"
        elif granularity == LockGranularity.PAGE_RANGE:
            return "PAGE_RANGE"
        elif granularity == LockGranularity.PAGE:
            return "PAGE"
        return "RECORD"


class _Transaction:
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

        # Format: {item_id: {"readers": set(), "writer": None}}
        self.table_locks = {}      # Locks on entire tables
        self.page_range_locks = {} # Locks on page ranges
        self.page_locks = {}       # Locks on specific pages
        self.record_locks = {}     # Locks on individual records


    def _get_lock_dict(self, granularity: LockGranularity):
        """
        Returns the appropriate lock dictionary for the given granularity level.
        """
        if granularity == LockGranularity.TABLE:
            return self.table_locks
        elif granularity == LockGranularity.PAGE_RANGE:
            return self.page_range_locks
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

        Args:
            transaction_id: ID of transaction requesting lock
            item_id: Resource identifier (e.g., "table/page_1/rid_2")
            mode: Type of lock requested (SHARED/EXCLUSIVE)
            granularity: Level of lock requested (TABLE/PAGE_RANGE/PAGE/RECORD)

        Returns:
            True if no conflicts with parent locks, False otherwise
        """
        # Split item_id into parts to check parent locks
        parts = item_id.split('/')
        print(f"\nChecking parent locks for T{transaction_id} requesting {LockMode.to_string(mode)} "
              f"lock on {LockGranularity.to_string(granularity)} {item_id}")

        # Build list of parent locks to check based on granularity
        parent_locks = []
        table_id = parts[0]

        # Build parent locks based on granularity level
        if granularity >= LockGranularity.RECORD:
            page_id = f"{parts[0]}/{parts[1]}/{parts[2]}/{parts[3]}"
            page_range_id = f"{parts[0]}/{parts[1]}"
            parent_locks = [
                (table_id, self.table_locks),
                (page_range_id, self.page_range_locks),
                (page_id, self.page_locks)
            ]
        elif granularity == LockGranularity.PAGE:
            page_range_id = f"{parts[0]}/{parts[1]}"
            parent_locks = [
                (table_id, self.table_locks),
                (page_range_id, self.page_range_locks)
            ]
        elif granularity == LockGranularity.PAGE_RANGE:
            parent_locks = [(table_id, self.table_locks)]

        # Check each parent lock for conflicts
        for lock_id, lock_dict in parent_locks:
            if (lock_id in lock_dict and
                    # Check if exclusive lock owned by another transaction
                    lock_dict[lock_id]["writer"] is not None and
                    # Check if exclusive lock is not owned by this transaction
                    lock_dict[lock_id]["writer"] != transaction_id):
                print(f"DENIED: {lock_id} is exclusively locked by T{lock_dict[lock_id]['writer']}")
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
                self.transactions[transaction_id] = _Transaction(transaction_id)

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

            # Check if parent locks conflict (hierarchical locking)
            if not self._check_parent_locks(transaction_id, item_id, mode, granularity):
                return False

            # Initialize lock info if this is first lock on item
            if item_id not in lock_dict:
                lock_dict[item_id] = {"readers": set(), "writer": None}

            lock_info = lock_dict[item_id]

            # print current lock state for debugging
            print(f"Current lock state for {item_id}:")
            print(f"  - Readers: {', '.join('T' + str(r) for r in lock_info['readers']) if lock_info['readers'] else 'None'}")
            writer_id = lock_info['writer']
            print(f"  - Writer: {'T' + str(writer_id) if writer_id is not None else 'None'}")

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
