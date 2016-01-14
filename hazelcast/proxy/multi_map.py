from hazelcast.proxy.base import Proxy


class MultiMap(Proxy):
    def put(self, key, value):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def remove(self, key, value):
        raise NotImplementedError

    def remove_all(self, key):
        raise NotImplementedError

    def value_count(self, key):
        raise NotImplementedError

    def size(self):
        raise NotImplementedError

    def key_set(self):
        raise NotImplementedError

    def values(self):
        raise NotImplementedError

    def entry_set(self):
        raise NotImplementedError

    def contains_key(self, key):
        raise NotImplementedError

    def contains_value(self, value):
        raise NotImplementedError

    def contains_entry(self, key, vlaue):
        raise NotImplementedError

    def clear(self):
        raise NotImplementedError

    def add_entry_listener(self, key=None, include_value=False, **kwargs):
        raise NotImplementedError

    def remove_entry_listener(self, registration_id):
        raise NotImplementedError

    def lock(self, key, lease_time=None):
        raise NotImplementedError

    def is_locked(self, key):
        raise NotImplementedError

    def try_lock(self, lease_time=None, timeout=None):
        raise NotImplementedError

    def unlock(self, key):
        raise NotImplementedError

    def force_unlock(self, key):
        raise NotImplementedError

    def __str__(self):
        return "MultiMap(name=%s)" % self.name
