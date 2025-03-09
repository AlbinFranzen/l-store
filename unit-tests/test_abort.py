import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lstore.transaction import Transaction

def test_abort():

    transaction = Transaction()

    transaction.queries.append(("query1", "table1", ["arg1", "arg2"]))
    transaction.queries.append(("query2", "table2", ["arg3", "arg4"]))

    transaction.changes.append(("table1", "key1", True))
    transaction.changes.append(("table2", "key2", False))

    result = transaction.abort()

    assert result == False

    assert len(transaction.changes) == 0

    assert len(transaction.held_locks) == 0

if __name__ == "__main__":
    test_abort()