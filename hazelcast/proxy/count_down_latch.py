from hazelcast.proxy.base import Proxy


class CountDownLatch(Proxy):
    def await(self, timeout):
        raise NotImplementedError

    def count_down(self):
        raise NotImplementedError

    def get_count(self):
        raise NotImplementedError

    def try_set_count(self):
        raise NotImplementedError

    def __str__(self):
        return "CountDownLatch(name=%s)" % self.name
