from config import POOL_SIZE

class BufferPool:
    def __init__(self, table_path):
        """
        Initialize buffer pool with specified size
        """
        self.table_path = table_path
        self.pool_size = POOL_SIZE
        self.frames = []  # List to store page frames [[page, is_dirty, pin_count],...]
        self.LRU_is_dirty = []  # List to track dirty pages for LRU
        self.LRU_not_dirty = []  # List to track clean pages for LRU
        
    def is_full(self):
        """
        Check if buffer pool is full
        """
        return len(self.frames) >= self.pool_size
    
    def add_frame(self, frame):
        """
        Add a new frame to the buffer pool
        """
        pass
