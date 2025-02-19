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

    def is_full(self):
        """
        Check if buffer pool is full
        """
        if len(self.frames) >= self.pool_size:
            return True
        else:
            return False
        
    def evict_page(self):
        """
        Check if buffer pool is full and evict a page if necessary
        """            
        if self.is_full():
            try: 
                LRU = self.frames[0] # Least Used Page
                frame_index = -1
                min_count = float('inf')

                # find page with least pin count in frames
                for i, frame in enumerate(self.frames):
                    if frame.pin_count < min_count:
                        min_count = frame.pin_count
                        LRU = frame
                        frame_index = i       

                # if all pages are pinned, return False
                if min_count > 0:
                    print("All pages are pinned")
                    return False
                
                # if page is dirty, write it to disk
                if LRU.is_dirty:
                    path = LRU.page_path
                    self.write_to_disk(path)

                # release memory from frames
                del self.frames[frame_index]
                return frame_index

            except Exception as e:
                return False
        
    def add_frame(self, frame):
        """
        Add a new frame to the buffer pool
        """
        #removes a frame if bufferpool is full
        #returns False if all frames are pinned (cannot evict)
        if len(self.frames) >= self.pool_size:
            if not self.evict():
                return False
            
        #adds a new frame to the bufferpool    
        self.frames.append(frame)

        #add frame to dirty list to track modified pages
        if frame[1]:
            self.LRU_is_dirty.append(frame)
        #add frame to clean list for easier eviction
        else:
            self.LRU_not_dirty.append(frame)
        
        return True

class Frame:
    def __init__(self, page=None, page_path=None):
        self.page = page
        self.page_path = page_path
        self.pin_count = 0
        self.dirty_bit = 0

    def set_page(self, new_page):
        self.page = new_page

    def clear_page(self):
        self.page = None

    def set_page_path(self, new_page_path):
        self.page_path = new_page_path

    def clear_page_path(self):
        self.page_path = None

    def increment_page_count(self):
        self.pin_count += 1

    def decrement_page_count(self):
        if self.pin_count > 0:
            self.pin_count -= 1

    def set_dirty_bit(self):
        self.dirty_bit = 1

    def clear_dirty_bit(self):
        self.dirty_bit = 0

    def is_empty(self):
        return self.page is None
