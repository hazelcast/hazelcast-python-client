from hazelcast.proxy.base import TransactionalProxy


class TransactionalQueue(TransactionalProxy):
    def offset(self, item, timeout=None):
        raise NotImplementedError

    def take(self):
        raise NotImplementedError

    def poll(self, timeout=None):
        raise NotImplementedError

    def peek(self, timeout=None):
        raise NotImplementedError

    def size(self):
        raise NotImplementedError

    def __str__(self):
        return "TransactionalQueue(name=%s)" % self.name
