from hazelcast.proxy.base import Proxy


class Semaphore(Proxy):

    def init(self, permits):
        raise NotImplementedError

    def acquire(self, permits=1):
        raise NotImplementedError

    def available_permits(self):
        raise NotImplementedError

    def drain_permits(self):
        raise NotImplementedError

    def reduce_permits(self, reduction):
        raise NotImplementedError

    def release(self, permits=1):
        raise NotImplementedError

    def try_acquire(self, permits=1, timeout=None):
        raise NotImplementedError

    def __str__(self):
        return "Semaphore(name=%s)" % self.name
