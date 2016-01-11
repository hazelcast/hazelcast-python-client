from hazelcast.proxy.base import Proxy


class Set(Proxy):
    def add(self, item):
        raise NotImplementedError

    def add_all(self, items):
        raise NotImplementedError

    def add_listener(self, item_added=None, item_removed=None):
        raise NotImplementedError

    def clear(self):
        raise NotImplementedError

    def contains(self, item):
        raise NotImplementedError

    def contains_all(self, items):
        raise NotImplementedError

    def get_all(self):
        raise NotImplementedError

    def is_empty(self):
        raise NotImplementedError

    def remove(self, item):
        raise NotImplementedError

    def remove_all(self, items):
        raise NotImplementedError

    def remove_listener(self, registration_id):
        raise NotImplementedError

    def retain_all(self, items):
        raise NotImplementedError

    def size(self):
        raise NotImplementedError

    def __str__(self):
        return "Set(name=%s)" % self.name
