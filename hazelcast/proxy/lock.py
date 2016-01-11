from hazelcast.proxy.base import Proxy


class Lock(Proxy):

    def force_unlock(self):
        raise NotImplementedError

    def get_lock_count(self):
        raise NotImplementedError

    def get_remaining_lease_time(self):
        raise NotImplementedError

    def is_locked(self):
        raise NotImplementedError

    def is_locked_by_current_thread(self):
        raise NotImplementedError

    def lock(self, lease_time=-1):
        raise NotImplementedError

    def try_lock(self, lease_time=-1, timeout=None):
        raise NotImplementedError

    def unlock(self):
        raise NotImplementedError

    def __str__(self):
        return "Lock(name=%s)" % self.name
