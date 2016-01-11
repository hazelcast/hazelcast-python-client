from hazelcast.proxy.base import Proxy


class List(Proxy):

    def add(self):
        raise NotImplementedError

    def add_at(self, index, item):
        raise NotImplementedError

    def add_all(self, items):
        raise NotImplementedError

    def add_all_at(self, index, items):
            raise NotImplementedError

    def add_listener(self, item_added=None, item_removed=None):
        raise NotImplementedError

    def clear(self):
        raise NotImplementedError

    def contains(self, item):
        raise NotImplementedError

    def contains_all(self, items):
        raise NotImplementedError

    def get(self, index):
        raise NotImplementedError

    def get_all(self):
        raise NotImplementedError

    def index_of(self, item):
        raise NotImplementedError

    def is_empty(self):
        raise NotImplementedError

    def last_index_of(self, item):
        raise NotImplementedError

    def remove(self, item):
        raise NotImplementedError

    def remove_at(self, index):
        raise NotImplementedError

    def remove_listener(self, registration_id):
        raise NotImplementedError

    def remove_all(self, items):
        raise NotImplementedError

    def retain_all(self, items):
        raise NotImplementedError

    def size(self):
        raise NotImplementedError

    def set_at(self, index, item):
        raise NotImplementedError

    def sub_list(self, from_index, to_index):
        raise NotImplementedError

    def __str__(self):
        return "List(name=%s)" % self.name
