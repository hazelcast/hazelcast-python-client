from hazelcast.proxy.base import Proxy


class AtomicLong(Proxy):
    def add_and_get(self, delta):
        raise NotImplementedError

    def compare_and_set(self, expect, update):
        raise NotImplementedError

    def decrement_and_get(self):
        raise NotImplementedError

    def get(self):
        raise NotImplementedError

    def get_and_add(self, delta):
        raise NotImplementedError

    def get_and_set(self, new_value):
        raise NotImplementedError

    def increment_and_get(self):
        raise NotImplementedError

    def get_and_increment(self):
        raise NotImplementedError

    def set(self, new_value):
        raise NotImplementedError

    def __str__(self):
        return "AtomicLong(name=%s)" % self.name
