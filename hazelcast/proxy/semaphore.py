from hazelcast.protocol.codec import \
    semaphore_acquire_codec, \
    semaphore_available_permits_codec, \
    semaphore_drain_permits_codec, \
    semaphore_init_codec, \
    semaphore_reduce_permits_codec, \
    semaphore_release_codec, \
    semaphore_try_acquire_codec

from hazelcast.proxy.base import PartitionSpecificProxy
from hazelcast.util import check_negative


class Semaphore(PartitionSpecificProxy):
    def init(self, permits):
        check_negative(permits, "Permits cannot be negative!")
        return self._encode_invoke_on_partition(semaphore_init_codec, permits=permits)

    def acquire(self, permits=1):
        check_negative(permits, "Permits cannot be negative!")
        return self._encode_invoke_on_partition(semaphore_acquire_codec, permits=permits)

    def available_permits(self):
        return self._encode_invoke_on_partition(semaphore_available_permits_codec)

    def drain_permits(self):
        return self._encode_invoke_on_partition(semaphore_drain_permits_codec)

    def reduce_permits(self, reduction):
        check_negative(reduction, "Reduction cannot be negative!")
        return self._encode_invoke_on_partition(semaphore_reduce_permits_codec, reduction=reduction)

    def release(self, permits=1):
        check_negative(permits, "Permits cannot be negative!")
        return self._encode_invoke_on_partition(semaphore_release_codec, permits=permits)

    def try_acquire(self, permits=1, timeout=0):
        check_negative(permits, "Permits cannot be negative!")
        t_msec = timeout * 1000
        return self._encode_invoke_on_partition(semaphore_try_acquire_codec, permits=permits, timeout=t_msec)

    def __str__(self):
        return "Semaphore(name=%s)" % self.name
