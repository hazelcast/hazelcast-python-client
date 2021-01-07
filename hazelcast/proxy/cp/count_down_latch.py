import uuid

from hazelcast.errors import OperationTimeoutError
from hazelcast.protocol.codec import (
    count_down_latch_await_codec,
    count_down_latch_get_round_codec,
    count_down_latch_count_down_codec,
    count_down_latch_get_count_codec,
    count_down_latch_try_set_count_codec,
)
from hazelcast.proxy.cp import BaseCPProxy
from hazelcast.util import to_millis, check_true, check_is_number, check_is_int


class CountDownLatch(BaseCPProxy):
    """A distributed, concurrent countdown latch data structure.

    CountDownLatch is a cluster-wide synchronization aid
    that allows one or more callers to wait until a set of operations being
    performed in other callers completes.

    CountDownLatch count can be reset using ``try_set_count()`` method after
    a countdown has finished but not during an active count. This allows
    the same latch instance to be reused.

    There is no ``await_latch()`` method to do an unbound wait since this is undesirable
    in a distributed application: for example, a cluster can split or the master
    and replicas could all die. In most cases, it is best to configure
    an explicit timeout so you have the ability to deal with these situations.

    All of the API methods in the CountDownLatch offer the exactly-once
    execution semantics. For instance, even if a ``count_down()`` call is
    internally retried because of crashed Hazelcast member, the counter
    value is decremented only once.
    """

    def await_latch(self, timeout):
        """Causes the current thread to wait until the latch has counted down to
        zero, or an exception is thrown, or the specified waiting time elapses.

        If the current count is zero then this method returns ``True``.

        If the current count is greater than zero, then the current
        thread becomes disabled for thread scheduling purposes and lies
        dormant until one of the following things happen:

        - The count reaches zero due to invocations of the ``count_down()`` method
        - This CountDownLatch instance is destroyed
        - The countdown owner becomes disconnected
        - The specified waiting time elapses

        If the count reaches zero, then the method returns with the
        value ``True``.

        If the specified waiting time elapses then the value ``False``
        is returned.  If the time is less than or equal to zero, the method
        will not wait at all.

        Args:
            timeout (int): The maximum time to wait in seconds

        Returns:
            hazelcast.future.Future[bool]: ``True`` if the count reached zero,
            ``False`` if the waiting time elapsed before the count reached zero
        Raises:
            IllegalStateError: If the Hazelcast instance was shut down while waiting.
        """
        check_is_number(timeout)
        timeout = max(0, timeout)
        invocation_uuid = uuid.uuid4()
        codec = count_down_latch_await_codec
        request = codec.encode_request(
            self._group_id, self._object_name, invocation_uuid, to_millis(timeout)
        )
        return self._invoke(request, codec.decode_response)

    def count_down(self):
        """Decrements the count of the latch, releasing all waiting threads if
        the count reaches zero.

        If the current count is greater than zero, then it is decremented.
        If the new count is zero:

        - All waiting threads are re-enabled for thread scheduling purposes
        - Countdown owner is set to ``None``.

        If the current count equals zero, then nothing happens.

        Returns:
            hazelcast.future.Future[None]:
        """
        invocation_uuid = uuid.uuid4()

        def handler(f):
            return self._do_count_down(f.result(), invocation_uuid)

        return self._get_round().continue_with(handler)

    def get_count(self):
        """Returns the current count.

        Returns:
            hazelcast.future.Future[int]: The current count.
        """
        codec = count_down_latch_get_count_codec
        request = codec.encode_request(self._group_id, self._object_name)
        return self._invoke(request, codec.decode_response)

    def try_set_count(self, count):
        """Sets the count to the given value if the current count is zero.

        If count is not zero, then this method does nothing and returns
        ``False``.

        Args:
            count (int): The number of times ``count_down()`` must be invoked
                before callers can pass through ``await_latch()``.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if the new count was set,
            ``False`` if the current count is not zero.
        """
        check_is_int(count)
        check_true(count > 0, "Count must be positive")
        codec = count_down_latch_try_set_count_codec
        request = codec.encode_request(self._group_id, self._object_name, count)
        return self._invoke(request, codec.decode_response)

    def _do_count_down(self, expected_round, invocation_uuid):
        def handler(f):
            try:
                f.result()
            except OperationTimeoutError:
                # we can retry safely because the retry is idempotent
                return self._do_count_down(expected_round, invocation_uuid)

        return self._request_count_down(expected_round, invocation_uuid).continue_with(handler)

    def _get_round(self):
        codec = count_down_latch_get_round_codec
        request = codec.encode_request(self._group_id, self._object_name)
        return self._invoke(request, codec.decode_response)

    def _request_count_down(self, expected_round, invocation_uuid):
        codec = count_down_latch_count_down_codec
        request = codec.encode_request(
            self._group_id, self._object_name, invocation_uuid, expected_round
        )
        return self._invoke(request)
