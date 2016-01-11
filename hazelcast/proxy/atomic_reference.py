from hazelcast.proxy.base import Proxy


class AtomicReference(Proxy):
    def __str__(self):
        return "AtomicReference(name=%s)" % self.name
