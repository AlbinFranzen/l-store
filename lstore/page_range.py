from page import Page
from config import *

# structure that defines list of valid base and tail pages
class PageRange:
    def __init__(self):
        self.base_pages = [Page()]
        self.tail_pages = [Page()]
        self.max_base_pages = PAGE_RANGE_SIZE  # amount defined in Milestone 1

    def __repr__(self):
        return f"\n\tbase pages:\n\t\t{self.base_pages} \n\ttail pages:\n\t\t{self.tail_pages}"

    # accessors
    def get_tail_pages(self):
        return self.tail_pages

    def get_base_pages(self):
        return self.base_pages

    def get_num_tail_pages(self):
        return len(self.tail_pages)

    def get_num_base_pages(self):
        return len(self.base_pages)

    # mutators
    def has_capacity(self):
        if self.get_num_base_pages() == self.max_base_pages:
            return False
        if not self.base_pages[-1].has_capacity():
            return False
        return True

    # inserts new tail page
    def insert_tail_page(self, new_tail_page):
        self.tail_pages.append(new_tail_page)

    # inserts new base page if there is capacity
    def insert_base_page(self, new_base_page):
        if self.base_page_has_capacity():
            self.base_pages.append(new_base_page)
        else:
            print(f"Maximum base page amount of ${self.max_base_pages} pages reached.")
            print(f"Unable to insert page ${new_base_page}")

    # deletes specified tail page if it exists
    def delete_tail_page(self, del_tail_page):
        if del_tail_page in self.tail_pages:
            self.tail_pages.remove(del_tail_page)
        else:
            print(f"Unable to delete tail page, ${del_tail_page} does not exist.")

    # deletes specified base page if it exists
    def delete_base_page(self, del_base_page):
        if del_base_page in self.base_pages:
            self.base_pages.remove(del_base_page)
        else:
            print(f"Unable to delete base page, ${del_base_page} does not exist.")

    # set tail pages via predefined list
    def set_tail_pages(self, tail_pages_list):
        self.tail_pages = tail_pages_list

    # set base pages via predefined list
    def set_base_pages(self, base_pages_list):
        if len(base_pages_list) < self.max_base_pages:
            self.base_pages = base_pages_list
        else:
            print(
                f"Unable to set base pages. Passed list exceeds max amount of base pages: ${self.max_base_pages}"
            )
            

