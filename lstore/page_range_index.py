# structure that defines list of valid base and tail pages
class PageRangeIndex:

    def __init__(self, base_start, base_end, tail_start, tail_end):
        self.base_pages = list(range(base_start, base_end))
        self.tail_pages = list(range(tail_start, tail_end))

    # accessors
    def get_tail_pages(self):
        return self.tail_pages

    def get_base_pages(self):
        return self.base_pages

    # mutators
    def set_tail_pages(self, tail_start, tail_end):
        self.tail_pages = list(range(tail_start, tail_end))

    def set_base_pages(self, base_start, base_end):
        self.base_pages = list(range(base_start, base_end))