class Frame:
    def __init__(self, page=None, page_path=None):
        self.page = page
        self.page_path = page_path
        self.pin_count = 0
        self.dirty_bit = 0

    def set_page(self, new_page):
        self.page = new_page
        self.set_dirty_bit()  # Mark as dirty when page is modified

    def clear_page(self):
        self.page = None
        self.set_dirty_bit()

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
