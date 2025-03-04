import os
from lstore.config import POOL_SIZE
from collections import OrderedDict
from lstore.page import Page

class BufferPool:
    def __init__(self, table_path):
        """
        Initialize buffer pool with specified size
        """
        self.table_path = table_path
        self.pool_size = POOL_SIZE
        self.io_count = 0
        self.frames = OrderedDict()  # {page_path: Frame}; Use OrderedDict to implement LRU - most recently used items are at the end
        
    def __repr__(self):
        frames_str = "\n".join(f"  {k}: {v}" for k, v in self.frames.items())
        return f"BufferPool(size={self.pool_size}) Frames:\n{frames_str}\n"

    def _update_lru(self, page_path):
        """
        Move accessed page to end of LRU order
        """
        if page_path in self.frames:
            frame = self.frames.pop(page_path)
            self.frames[page_path] = frame
        

    def evict_page(self):
        """
        Evict least recently used unpinned page, prioritizing non-dirty pages
        Returns:
            True if page was evicted or space was available, False if no page could be evicted
        """            
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
                    self.write_to_disk(page_path, frame.page)            
                    del self.frames[page_path]
                    return True
                    
            # If we get here, all pages are pinned
            print("Warning: All pages are pinned, cannot evict")
            return False

        except Exception as e:
            print(f"Error during page eviction: {e}")
            return False
        

    def add_frame(self, page_path, page_data=None):
        
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
        if len(self.frames) >= self.pool_size and not self.evict_page():
            return None
            
        # Read page data from disk
        if page_data is None:
            page_data = self.read_from_disk(page_path)
            if page_data is None:
                return None
        
        # Create new frame and add to pool
        new_frame = Frame(page=page_data, page_path=page_path)
        self.frames[page_path] = new_frame
        return new_frame


    def abs_remove_frame(self, page_path):
        """
        Remove a frame from the buffer pool without writing to disk
        Args:
            page_path: path to the page file
        Returns:
            True if successful, False if error
        """
        if page_path in self.frames:            
            del self.frames[page_path]
            return True
        return False


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
                f.write(page.serialize())
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
        self.io_count += 1
        try:
            if not os.path.exists(page_path):
                return None
                
            with open(page_path, 'rb') as f:
                data = f.read()
            return Page().deserialize(data)
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
            frame.increment_pin_count()
            self._update_lru(page_path)
        
            return frame.page

        # Not in buffer pool, try to add it
        frame = self.add_frame(page_path)
        if frame:
            frame.increment_pin_count()
            return frame.page

        return None


    def update_page(self, page_path, make_dirty=False):
        """
        Update a page in the buffer pool or disk
        Args:
            page_path: path to the page file
            data: data to write
        Returns:
            True if successful, False if error
        """
        # Check if page is in buffer pool
        frame = self.frames.get(page_path)
        if frame and make_dirty:
            frame.set_dirty_bit()

        return True


    def unpin_page(self, page_path):
        """
        Decrement pin count for a page
        """
        if page_path in self.frames:
            self.frames[page_path].decrement_pin_count()


    def mark_dirty(self, page_path):
        """
        Mark a page as dirty
        """
        if page_path in self.frames:
            self.frames[page_path].set_dirty_bit()
            

    def rename_frame(self, old_path, new_path):
        """
        Rename a frame in the buffer pool atomicially
        """
        # Check if the original frame exists
        if old_path not in self.frames:
            return False
            
        # Get the frame without removing it first (to avoid race conditions)
        frame = self.frames[old_path]
        
        # Create a copy of the frame with updated path
        frame.set_page_path(new_path)
        
        # Add the frame under the new key
        self.frames[new_path] = frame
        
        # Now remove the old key (only after new one is added)
        del self.frames[old_path]
    
        return True


class Frame:
    def __init__(self, page=None, page_path=None):
        self.page = page
        self.page_path = page_path
        self.pin_count = 0
        self.dirty_bit = 0
        
    def __repr__(self):
        return f"Frame({self.page_path}) Pin count: {self.pin_count} Dirty: {self.dirty_bit}"

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
