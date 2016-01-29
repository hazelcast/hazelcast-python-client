from hazelcast.protocol.codec import lock_force_unlock_codec, lock_get_lock_count_codec, \
    lock_get_remaining_lease_time_codec, lock_is_locked_by_current_thread_codec, lock_is_locked_codec, lock_lock_codec, \
    lock_try_lock_codec, lock_unlock_codec
from hazelcast.proxy.base import PartitionSpecificProxy
from hazelcast.util import thread_id, to_millis


class Lock(PartitionSpecificProxy):
    def force_unlock(self):
        return self._encode_invoke(lock_force_unlock_codec)

    def get_lock_count(self):
        return self._encode_invoke(lock_get_lock_count_codec)

    def get_remaining_lease_time(self):
        return self._encode_invoke(lock_get_remaining_lease_time_codec)

    def is_locked(self):
        return self._encode_invoke(lock_is_locked_codec)

    def is_locked_by_current_thread(self):
        return self._encode_invoke(lock_is_locked_by_current_thread_codec,
                                   thread_id=thread_id())

    def lock(self, lease_time=-1):
        return self._encode_invoke(lock_lock_codec, lease_time=to_millis(lease_time),
                                   thread_id=thread_id())

    def try_lock(self, timeout=0, lease_time=-1):
        return self._encode_invoke(lock_try_lock_codec, lease=to_millis(lease_time),
                                   thread_id=thread_id(), timeout=to_millis(timeout))

    def unlock(self):
        return self._encode_invoke(lock_unlock_codec, thread_id=thread_id())
