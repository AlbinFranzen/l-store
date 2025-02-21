from lstore.config import POOL_SIZE
from collections import OrderedDict
import os

from lstore.page import Page


class BufferPool:
    def __init__(self, table_path):
        """
        Initialize buffer pool with specified size
        """
        self.table_path = table_path
        self.pool_size = POOL_SIZE
        # Use OrderedDict to implement LRU - most recently used items are at the end
        self.frames = OrderedDict()  # {page_path: Frame}
        
    def _update_lru(self, page_path):
        """
        Move accessed page to end of LRU order
        """
        if page_path in self.frames:
            frame = self.frames.pop(page_path)
            self.frames[page_path] = frame
            
    def is_full(self):
        """
        Check if buffer pool is full
        """
        return len(self.frames) >= self.pool_size
        
    def evict_page(self):
        """
        Evict least recently used unpinned page, prioritizing non-dirty pages
        Returns:
            True if page was evicted, False if no page could be evicted
        """            
        if not self.is_full():
            return True
        
        try:
            # First try to evict non-dirty pages
            for page_path, frame in list(self.frames.items()):
                if frame.pin_count == 0 and not frame.dirty_bit:
                    del self.frames[page_path]
                    return True

            # If no non-dirty pages available, try dirty pages
            for page_path, frame in list(self.frames.items()):
                if frame.pin_count == 0:
                    # Write dirty page to disk
                    if not self.write_to_disk(page_path, frame.page):
                        continue  # Try next frame if write fails
                    
                    del self.frames[page_path]
                    return True
                    
            # If we get here, all pages are pinned
            print("Warning: All pages are pinned, cannot evict")
            return False

        except Exception as e:
            print(f"Error during page eviction: {e}")
            return False
        
    def add_frame(self, page_path):
        """
        Add a new frame to the buffer pool using a page path
        Args:
            page_path: path to the page file
        Returns:
            Frame object if successful, None if error
        """
        # Check if frame already exists
        if page_path in self.frames:
            self._update_lru(page_path)
            return self.frames[page_path]
            
        # Try to make space if needed
        if self.is_full() and not self.evict_page():
            return None
            
        # Read page data from disk
        page_data = self.read_from_disk(page_path)
        if page_data is None:
            return None
        
        # Create new frame and add to pool
        new_frame = Frame(page=page_data, page_path=page_path)
        self.frames[page_path] = new_frame
        return new_frame

    def write_to_disk(self, page_path, page):
        """
        Write a page to disk
        Args:
            page_path: path to the page file
            page: page object to write
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(page_path), exist_ok=True)
            
            with open(page_path, 'wb') as f:
                f.write(page.serialize())  # need to implement page serializing
                f.flush()
                os.fsync(f.fileno())
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
            Page object or None if error
        """
        try:
            if not os.path.exists(page_path):
                return None
                
            with open(page_path, 'rb') as f:
                data = f.read()
            return Page.deserialize(data)  # need to implement page de-serializing
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
        # Try to get from buffer pool first
        frame = self.frames.get(page_path)
        if frame:
            frame.increment_page_count()
            self._update_lru(page_path)
            return frame.page

        # Not in buffer pool, try to add it
        frame = self.add_frame(page_path)
        if frame:
            frame.increment_pin_count()
            return frame.page
            
        return None
        
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
    
    def unpin_page(self, page_path):
        """
        Decrement pin count for a page
        """
        if page_path in self.frames:
            self.frames[page_path].decrement_page_count()
            
    def mark_dirty(self, page_path):
        """
        Mark a page as dirty
        """
        if page_path in self.frames:
            self.frames[page_path].set_dirty_bit()

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

    def increment_pin_count(self):
        self.pin_count += 1

    def decrement_pin_count(self):
        if self.pin_count > 0:
            self.pin_count -= 1

    def set_dirty_bit(self):
        self.dirty_bit = 1

    def clear_dirty_bit(self):
        self.dirty_bit = 0

    def is_empty(self):
        return self.page is None
