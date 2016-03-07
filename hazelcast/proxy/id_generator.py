import threading

from hazelcast.proxy.base import Proxy
from hazelcast.util import AtomicInteger

BLOCK_SIZE = 10000


class IdGenerator(Proxy):
    def __init__(self, client, service_name, name, atomic_long):
        super(IdGenerator, self).__init__(client, service_name, name)
        self._atomic_long = atomic_long
        self._residue = BLOCK_SIZE
        self._local = -1
        self._lock = threading.RLock()

    def _on_destroy(self):
        self._atomic_long.destroy()

    def init(self, initial):
        if id <= 0:
            return False
        step = initial / BLOCK_SIZE
        with self._lock:
            init = self._atomic_long.compare_and_set(0, step + 1).result()
            if init:
                self._local = step
                self._residue = (initial % BLOCK_SIZE) + 1
            return init

    def new_id(self):
        with self._lock:
            curr = self._residue
            self._residue += 1
            if self._residue >= BLOCK_SIZE:
                increment = self._atomic_long.get_and_increment().result()
                self._local = increment
                self._residue = 0
                return self.new_id()
            return self._local * BLOCK_SIZE + curr

