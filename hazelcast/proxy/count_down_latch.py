from hazelcast.protocol.codec import \
    count_down_latch_await_codec, \
    count_down_latch_count_down_codec, \
    count_down_latch_get_count_codec, \
    count_down_latch_try_set_count_codec

from hazelcast.proxy.base import PartitionSpecificProxy
from hazelcast.util import check_negative


class CountDownLatch(PartitionSpecificProxy):
    def await(self, timeout):
        t_msec = timeout * 1000
        return self._encode_invoke_on_partition(count_down_latch_await_codec, timeout=t_msec)

    def count_down(self):
        return self._encode_invoke_on_partition(count_down_latch_count_down_codec)

    def get_count(self):
        return self._encode_invoke_on_partition(count_down_latch_get_count_codec)

    def try_set_count(self, count):
        check_negative(count, "count can't be negative")
        return self._encode_invoke_on_partition(count_down_latch_try_set_count_codec, count=count)

    def __str__(self):
        return "CountDownLatch(name=%s)" % self.name
