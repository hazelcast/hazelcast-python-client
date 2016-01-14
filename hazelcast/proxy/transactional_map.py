from hazelcast.proxy.base import TransactionalProxy


class TransactionalMap(TransactionalProxy):
    def contains_key(self, key):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def get_for_update(self, key):
        raise NotImplementedError

    def size(self):
        raise NotImplementedError

    def is_empty(self):
        raise NotImplementedError

    def put(self, key, value, ttl=None):
        raise NotImplementedError

    def set(self, key, value):
        raise NotImplementedError

    def put_if_absent(self, key, value):
        raise NotImplementedError

    def replace(self, key, value):
        raise NotImplementedError

    def replace_if_same(self, key, old_value, new_value):
        raise NotImplementedError

    def remove(self, key):
        raise NotImplementedError

    def remove_if_same(self, key, value):
        raise NotImplementedError

    def delete(self, key):
        raise NotImplementedError

    def key_set(self, predicate=None):
        raise NotImplementedError

    def values(self, predicate=None):
        raise NotImplementedError

    def __str__(self):
        return "TransactionalMap(name=%s)" % self.name
