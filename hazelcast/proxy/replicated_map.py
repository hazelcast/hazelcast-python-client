from hazelcast.proxy.base import Proxy


class ReplicatedMap(Proxy):
    def __str__(self):
        return "ReplicatedMap(name=%s)" % self.name
