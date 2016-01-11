from hazelcast.proxy.base import Proxy


class AtomicLong(Proxy):

    def __str__(self):
        return "AtomicLong(name=%s)" % self.name
