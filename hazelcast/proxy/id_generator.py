from hazelcast.proxy.base import Proxy


class IdGenerator(Proxy):
    def init(self, id):
        raise NotImplementedError

    def new_id(self):
        raise NotImplementedError

    def __str__(self):
        return "IdGenerator(name=%s)" % self.name
