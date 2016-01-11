from hazelcast.proxy.base import Proxy


class CountDownLatch(Proxy):
    def __str__(self):
        return "CountDownLatch(name=%s)" % self.name
