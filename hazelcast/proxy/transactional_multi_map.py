from hazelcast.proxy.base import TransactionalProxy


class TransactionalMultiMap(TransactionalProxy):

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

    def __str__(self):
        return "TransactionalMultiMap(name=%s)" % self.name
