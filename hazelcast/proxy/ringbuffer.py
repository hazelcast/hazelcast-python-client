from hazelcast.proxy.base import Proxy


class Ringbuffer(Proxy):
    def __str__(self):
        return "Ringbuffer(name=%s)" % self.name
