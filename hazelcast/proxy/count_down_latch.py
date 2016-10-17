from hazelcast.protocol.codec import \
    count_down_latch_await_codec, \
    count_down_latch_count_down_codec, \
    count_down_latch_get_count_codec, \
    count_down_latch_try_set_count_codec

from hazelcast.proxy.base import PartitionSpecificProxy
from hazelcast.util import check_not_negative, to_millis


class CountDownLatch(PartitionSpecificProxy):
    """
    CountDownLatch is a backed-up, distributed, cluster-wide synchronization aid that allows one or more threads to wait until a
    set of operations being performed in other threads completes
    """
    def await(self, timeout):
        """
        Causes the current thread to wait until the latch has counted down to zero, or the specified waiting time
        elapses.

        If the current count is zero then this method returns immediately with the value ``true``.

        If the current count is greater than zero, then the current thread becomes disabled for thread scheduling
        purposes and lies dormant until one of following happens:

            * the count reaches zero due to invocations of the countDown() method,
            * this CountDownLatch instance is destroyed,
            * the countdown owner becomes disconnected,
            * some other thread interrupts the current thread, or
            * the specified waiting time elapses.

        If the count reaches zero, then the method returns with the value ``true``.

        :param timeout: (long), the maximum time in seconds to wait.
        :return: (bool), ``true`` if the count reached zero, ``false`` if the waiting time elapsed before the count reached zero.
        """
        return self._encode_invoke(count_down_latch_await_codec, timeout=to_millis(timeout))

    def count_down(self):
        """
        Decrements the count of the latch, releasing all waiting threads if the count reaches zero.

        If the current count is greater than zero, then it is decremented. If the new count is zero:
            * All waiting threads are re-enabled for thread scheduling purposes, and
            * Countdown owner is set to ``None``.

        If the current count equals zero, nothing happens.
        """
        return self._encode_invoke(count_down_latch_count_down_codec)

    def get_count(self):
        """
        Returns the current count

        :return: (int), the current count.
        """
        return self._encode_invoke(count_down_latch_get_count_codec)

    def try_set_count(self, count):
        """
        Sets the count to the given value if the current count is zero. If count is not zero, this method does nothing
        and returns ``false``.

        :param count: (int), the number of times count_down() must be invoked before threads can pass through await().
        :return: (bool), ``true`` if the new count was set, ``false`` if the current count is not zero.
        """
        check_not_negative(count, "count can't be negative")
        return self._encode_invoke(count_down_latch_try_set_count_codec, count=count)

