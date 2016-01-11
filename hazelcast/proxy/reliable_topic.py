from hazelcast.proxy.base import Proxy


class ReliableTopic(Proxy):
    def __str__(self):
        return "ReliableTopic(name=%s)" % self.name
