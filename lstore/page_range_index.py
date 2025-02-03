# structure that defines list of valid base and tail pages
import config

class PageRangeIndex:

    def __init__(self):
        self.base_pages = []
        self.tail_pages = []
        self.max_base_pages = config.PAGE_RANGE_SIZE    # amount defined in Milestone 1

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
    def base_page_has_capacity(self):
        return self.get_num_base_pages() < self.max_base_pages

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
            print(f"Unable to set base pages. Passed list exceeds max amount of base pages: ${self.max_base_pages}")