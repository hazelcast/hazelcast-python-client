from hazelcast.proxy.base import Proxy


class AtomicReference(Proxy):
    def compare_and_set(self, expect, update):
        raise NotImplementedError

    def get(self):
        raise NotImplementedError

    def set(self, new_value):
        raise NotImplementedError

    def get_and_set(self, new_value):
        raise NotImplementedError

    def set_and_get(self, new_value):
        raise NotImplementedError

    def is_null(self):
        raise NotImplementedError

    def clear(self):
        raise NotImplementedError

    def contains(self):
        raise NotImplementedError

    def __str__(self):
        return "AtomicReference(name=%s)" % self.name
