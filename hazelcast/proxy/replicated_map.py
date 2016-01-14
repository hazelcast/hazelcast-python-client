from hazelcast.proxy.base import Proxy


class ReplicatedMap(Proxy):
    def add_entry_listener(self, include_value=False, key=None, predicate=None, **kwargs):
        raise NotImplementedError

    def clear(self):
        raise NotImplementedError

    def contains_key(self, key):
        raise NotImplementedError

    def contains_value(self, value):
        raise NotImplementedError

    def delete(self, key):
        raise NotImplementedError

    def entry_set(self, predicate=None):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def is_empty(self):
        raise NotImplementedError

    def key_set(self):
        raise NotImplementedError

    def put(self, key, value, ttl=-1):
        raise NotImplementedError

    def put_all(self, map):
        raise NotImplementedError

    def remove(self, key):
        raise NotImplementedError

    def remove_entry_listener(self, registration_id):
        raise NotImplementedError

    def size(self):
        raise NotImplementedError

    def values(self):
        raise NotImplementedError

    def __str__(self):
        return "ReplicatedMap(name=%s)" % self.name
