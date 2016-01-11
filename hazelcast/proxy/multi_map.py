from hazelcast.proxy.base import Proxy


class MultiMap(Proxy):
    def __str__(self):
        return "MultiMap(name=%s)" % self.name
