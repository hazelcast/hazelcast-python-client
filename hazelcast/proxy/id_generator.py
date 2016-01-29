import threading

from hazelcast.proxy.base import Proxy
from hazelcast.util import AtomicInteger

BLOCK_SIZE = 10000


class IdGenerator(Proxy):
    def __init__(self, client, service_name, name, atomic_long):
        super(IdGenerator, self).__init__(client, service_name, name)
        self._atomic_long = atomic_long
        self._residue = AtomicInteger(BLOCK_SIZE)
        self._local = AtomicInteger(-1)
        self._lock = threading.RLock()

    def _on_destroy(self):
        self._atomic_long.destroy()

    def init(self, initial):
        if id <= 0:
            return False
        step = initial / BLOCK_SIZE
        with self._lock:
            init = self._atomic_long.compare_and_set(0, step + 1)
            if init:
                self._local.set(step)
                self._residue.set((initial % BLOCK_SIZE) + 1)
            return init

    def new_id(self):
        val = self._residue.get_and_increment()
        if val >= BLOCK_SIZE:
            with self._lock:
                val = self._residue.get()
                if val >= BLOCK_SIZE:
                    increment = self._atomic_long.get_and_increment()
                    self._local.set(increment)
                    self._residue.set(0)
                return self.new_id()
        get = self._local.get()
        return get * BLOCK_SIZE + val

