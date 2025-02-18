from config import POOL_SIZE
import os

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
        
    def evict(self):
        """
        Check if buffer pool is full
        """            
        if len(self.frames) >= self.pool_size:
            try: 
                # find pages with least pin count in frames
                min_pin = float('inf')
                for frame in self.frames:
                    min_pin = min(min_pin, frame[2])

                # find pages with min_pin in frames
                candidates = []
                for frame in self.frames:
                    if frame[2] == min_pin:
                        candidates.append(frame)

                if not candidates:
                    return False
                
                # find clean pages in candidates
                for frame in candidates:
                    if not frame[1]:
                            selected_frame = frame

                # if no clean pages, find dirty pages in candidates
                if selected_frame is None:
                    for frame in candidates:
                        if frame[1]:
                                selected_frame = frame
                    if selected_frame:
                        self.write_to_disk(selected_frame)

                # remove selected frame from frames
                if selected_frame:
                    self.frames.remove(selected_frame)
                    if not selected_frame[1]:
                        self.LRU_not_dirty.remove(selected_frame)
                    else:
                        self.LRU_is_dirty.remove(selected_frame)
                    return True
                return False

            except Exception as e:
                return False
        
        return False
    
    def write_to_disk(self, page):
        """
        Write page to disk
        """

    def add_frame(self, frame):
        """
        Add a new frame to the buffer pool
        """
        pass
