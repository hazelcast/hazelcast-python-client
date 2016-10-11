from hazelcast.protocol.codec import \
    semaphore_acquire_codec, \
    semaphore_available_permits_codec, \
    semaphore_drain_permits_codec, \
    semaphore_init_codec, \
    semaphore_reduce_permits_codec, \
    semaphore_release_codec, \
    semaphore_try_acquire_codec
from hazelcast.proxy.base import PartitionSpecificProxy
from hazelcast.util import check_not_negative, to_millis


class Semaphore(PartitionSpecificProxy):
    """
    Semaphore is a backed-up distributed alternative to the Python `asyncio.Semaphore <https://docs.python.org/3/library/asyncio-sync.html>`_

    Semaphore is a cluster-wide counting semaphore. Conceptually, it maintains a set of permits. Each acquire() blocks
    if necessary until a permit is available, and then takes it. Each release() adds a permit, potentially releasing a
    blocking acquirer. However, no actual permit objects are used; the semaphore just keeps a count of the number
    available and acts accordingly.

    The Hazelcast distributed semaphore implementation guarantees that threads invoking any of the acquire methods are
    selected to obtain permits in the order in which their invocation of those methods was processed(first-in-first-out;
    FIFO). Note that FIFO ordering necessarily applies to specific internal points of execution within the cluster.
    Therefore, it is possible for one member to invoke acquire before another, but reach the ordering point after the
    other, and similarly upon return from the method.

    This class also provides convenience methods to acquire and release multiple permits at a time. Beware of the
    increased risk of indefinite postponement when using the multiple acquire. If a single permit is released to a
    semaphore that is currently blocking, a thread waiting for one permit will acquire it before a thread waiting for
    multiple permits regardless of the call order.
    """
    def init(self, permits):
        """
        Try to initialize this Semaphore instance with the given permit count.

        :param permits: (int), the given permit count.
        :return: (bool), ``true`` if initialization success.
        """
        check_not_negative(permits, "Permits cannot be negative!")
        return self._encode_invoke(semaphore_init_codec, permits=permits)

    def acquire(self, permits=1):
        """
        Acquires one or specified amount of permits if available, and returns immediately, reducing the number of
        available permits by one or given amount.

        If insufficient permits are available then the current thread becomes disabled for thread scheduling purposes
        and lies dormant until one of following happens:

            * some other thread invokes one of the release methods for this semaphore, the current thread is next to be
            assigned permits and the number of available permits satisfies this request,
            * this Semaphore instance is destroyed, or
            * some other thread interrupts the current thread.

        :param permits: (int), the number of permits to acquire (optional).
        """
        check_not_negative(permits, "Permits cannot be negative!")
        return self._encode_invoke(semaphore_acquire_codec, permits=permits)

    def available_permits(self):
        """
        Returns the current number of permits currently available in this semaphore.

            * This method is typically used for debugging and testing purposes.
        :return: (int), the number of available permits in this semaphore.
        """
        return self._encode_invoke(semaphore_available_permits_codec)

    def drain_permits(self):
        """
        Acquires and returns all permits that are immediately available.

        :return: (int), the number of permits drained.
        """
        return self._encode_invoke(semaphore_drain_permits_codec)

    def reduce_permits(self, reduction):
        """
        Shrinks the number of available permits by the indicated reduction. This method differs from acquire in that it
        does not block waiting for permits to become available.

        :param reduction: (int),  the number of permits to remove.
        """
        check_not_negative(reduction, "Reduction cannot be negative!")
        return self._encode_invoke(semaphore_reduce_permits_codec, reduction=reduction)

    def release(self, permits=1):
        """
        Releases one or given number of permits, increasing the number of available permits by one or that amount.

        There is no requirement that a thread that releases a permit must have acquired that permit by calling one of
        the acquire methods. Correct usage of a semaphore is established by programming convention in the application.

        :param permits: (int), the number of permits to release (optional).
        """
        check_not_negative(permits, "Permits cannot be negative!")
        return self._encode_invoke(semaphore_release_codec, permits=permits)

    def try_acquire(self, permits=1, timeout=0):
        """
        Tries to acquire one or the given number of permits, if they are available, and returns immediately, with the
        value ``true``, reducing the number of available permits by the given amount.

        If there are insufficient permits and a timeout is provided, the current thread becomes disabled for thread
        scheduling purposes and lies dormant until one of following happens:
            * some other thread invokes the release() method for this semaphore and the current thread is next to be
            assigned a permit, or
            * some other thread interrupts the current thread, or
            * the specified waiting time elapses.

        If there are insufficient permits and no timeout is provided, this method will return immediately with the value
        ``false`` and the number of available permits is unchanged.

        :param permits: (int), the number of permits to acquire (optional).
        :param timeout: (long), the maximum time in seconds to wait for the permit(s) (optional).
        :return: (bool), ``true`` if desired amount of permits was acquired, ``false`` otherwise.
        """
        check_not_negative(permits, "Permits cannot be negative!")
        return self._encode_invoke(semaphore_try_acquire_codec, permits=permits, timeout=to_millis(timeout))
