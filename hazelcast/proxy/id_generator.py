from hazelcast.proxy.base import Proxy


class IdGenerator(Proxy):
    def __str__(self):
        return "IdGenerator(name=%s)" % self.name
