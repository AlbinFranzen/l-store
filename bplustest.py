from bplustree import BPlusTree
tree = BPlusTree('/tmp/bplustree.db', order=50)
tree[1] = b'rid1'
tree[1] = b'rid1,rid2'
print(tree[1])
