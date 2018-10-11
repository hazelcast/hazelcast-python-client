from hazelcast.protocol.codec import lock_force_unlock_codec, lock_get_lock_count_codec, \
    lock_get_remaining_lease_time_codec, lock_is_locked_by_current_thread_codec, lock_is_locked_codec, lock_lock_codec, \
    lock_try_lock_codec, lock_unlock_codec
from hazelcast.proxy.base import PartitionSpecificProxy, MAX_SIZE
from hazelcast.util import thread_id, to_millis


class Lock(PartitionSpecificProxy):
    """
    Distributed implementation of `asyncio.Lock <https://docs.python.org/3/library/asyncio-sync.html>`_
    """

    def __init__(self, client, service_name, name):
        super(Lock, self).__init__(client, service_name, name)
        self.reference_id_generator = self._client.lock_reference_id_generator

    def force_unlock(self):
        """
        Releases the lock regardless of the lock owner. It always successfully unlocks, never blocks, and returns
        immediately.
        """
        return self._encode_invoke(lock_force_unlock_codec, reference_id=self.reference_id_generator.get_next())

    def get_lock_count(self):
        """
        Returns re-entrant lock hold count, regardless of lock ownership.

        :return: (int), the lock hold count.
        """
        return self._encode_invoke(lock_get_lock_count_codec)

    def get_remaining_lease_time(self):
        """
        Returns remaining lease time in milliseconds. If the lock is not locked then -1 will be returned.

        :return: (long), remaining lease time in milliseconds.
        """
        return self._encode_invoke(lock_get_remaining_lease_time_codec)

    def is_locked(self):
        """
        Returns whether this lock is locked or not.

        :return: (bool), ``true`` if this lock is locked, ``false`` otherwise.
        """
        return self._encode_invoke(lock_is_locked_codec)

    def is_locked_by_current_thread(self):
        """
        Returns whether this lock is locked by current thread or not.

        :return: (bool), ``true`` if this lock is locked by current thread, ``false`` otherwise.
        """
        return self._encode_invoke(lock_is_locked_by_current_thread_codec,
                                   thread_id=thread_id())

    def lock(self, lease_time=-1):
        """
        Acquires the lock. If a lease time is specified, lock will be released after this lease time.

        If the lock is not available, the current thread becomes disabled for thread scheduling purposes and lies
        dormant until the lock has been acquired.

        :param lease_time: (long), time to wait before releasing the lock (optional).
        """
        return self._encode_invoke(lock_lock_codec, invocation_timeout=MAX_SIZE, lease_time=to_millis(lease_time),
                                   thread_id=thread_id(), reference_id=self.reference_id_generator.get_next())

    def try_lock(self, timeout=0, lease_time=-1):
        """
        Tries to acquire the lock. When the lock is not available,

            * If timeout is not provided, the current thread doesn't wait and returns ``false`` immediately.
            * If a timeout is provided, the current thread becomes disabled for thread scheduling purposes and lies
            dormant until one of the followings happens:
                * the lock is acquired by the current thread, or
                * the specified waiting time elapses.

        If lease time is provided, lock will be released after this time elapses.

        :param timeout: (long), maximum time in seconds to wait for the lock (optional).
        :param lease_time: (long), time in seconds to wait before releasing the lock (optional).
        :return: (bool), ``true`` if the lock was acquired and otherwise, ``false``.
        """
        return self._encode_invoke(lock_try_lock_codec, invocation_timeout=MAX_SIZE, lease=to_millis(lease_time),
                                   thread_id=thread_id(), timeout=to_millis(timeout),
                                   reference_id=self.reference_id_generator.get_next())

    def unlock(self):
        """
        Releases the lock.
        """
        return self._encode_invoke(lock_unlock_codec, thread_id=thread_id(),
                                   reference_id=self.reference_id_generator.get_next())
