from hazelcast.proxy.base import Proxy

OVERFLOW_POLICY_OVERWRITE = 0
OVERFLOW_POLICY_FAIL = 1


class Ringbuffer(Proxy):
    def capacity(self):
        raise NotImplementedError

    def size(self):
        raise NotImplementedError

    def tail_sequence(self):
        raise NotImplementedError

    def head_sequence(self):
        raise NotImplementedError

    def remaining_capacity(self):
        raise NotImplementedError

    def add(self, item, overflow_policy=OVERFLOW_POLICY_OVERWRITE):
        raise NotImplementedError

    def add_all(self, items, overflow_policy):
        raise NotImplementedError

    def read_one(self, sequence):
        raise NotImplementedError

    def read_many(self, start_sequence, min_count, max_count):
        raise NotImplementedError

    def __str__(self):
        return "Ringbuffer(name=%s)" % self.name
