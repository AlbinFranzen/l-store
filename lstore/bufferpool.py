from lstore.config import POOL_SIZE
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
        
    def add_frame(self, page_path):
        """
        Add a new frame to the buffer pool using a page path
        Args:
            page_path: path to the page file
        Returns:
            Frame object if successful, None if error
        """
        # Check if bufferpool is full and try to evict if needed
        if len(self.frames) >= self.pool_size:
            if not self.evict_page():
                return None
            
        # Read page data from disk
        page_data = self.read_from_disk(page_path)
        if page_data is None:
            return None
        
        # Create new frame with page data
        new_frame = Frame(page=page_data, page_path=page_path)
        
        # Add frame to buffer pool
        self.frames.append(new_frame)
        return new_frame

    def write_to_disk(self, page_path, page):
        """
        Write a page to disk
        Args:
            page_path: path to the page file
            data: data to write
        """
        try:
            with open(page_path, 'wb') as f:
                f.write(page)
                f.flush()
                os.fsync(f.fileno())  # Ensure data is written to disk
            return True
        except Exception as e:
            print(f"Error writing to disk: {e}")
            return False

    def read_from_disk(self, page_path):
        """
        Read a page from disk
        Args:
            page_path: path to the page file
        Returns:
            data read from disk or None if error
        """
        try:
            with open(page_path, 'rb') as f:
                data = f.read()
            return data
        except Exception as e:
            print(f"Error reading from disk: {e}")
            return None

    def get_page(self, page_path):
        """
        Get a page from buffer pool or disk
        Args:
            page_path: path to the page file
        Returns:
            page data or None if error
        """
        # Check if page is in buffer pool
        for frame in self.frames:
            if frame.page_path == page_path:
                frame.increment_page_count()
                return frame.page

        # If not in buffer pool, read from disk
        page_data = self.read_from_disk(page_path)
        if page_data is None:
            return None

        # Create new frame
        new_frame = Frame(page=page_data, page_path=page_path)
        
        # Add to buffer pool
        if not self.add_frame(page_path):
            return None
        
        return page_data
    
    def update_page(self, page_path, new_page):
        """
        Update a page in the buffer pool or disk
        Args:
            page_path: path to the page file
            data: data to write
        Returns:
            True if successful, False if error
        """
        # Check if page is in buffer pool
        for frame in self.frames:
            if frame.page_path == page_path:
                frame.set_page(new_page)
                frame.set_dirty_bit()
                return True

        # If not in buffer pool, write to disk
        if not self.write_to_disk(page_path, new_page):
            return False

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
