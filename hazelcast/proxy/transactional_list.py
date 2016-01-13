from hazelcast.proxy.base import TransactionalProxy


class TransactionalList(TransactionalProxy):

    def add(self, item):
        raise NotImplementedError

    def remove(self, item):
        raise NotImplementedError

    def size(self):
        raise NotImplementedError
    
    def __str__(self):
        return "TransactionalList(name=%s)" % self.name
