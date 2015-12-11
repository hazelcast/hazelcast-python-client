from __future__ import with_statement
import threading


def check_not_none(val, message):
    if val is None:
        raise AssertionError(message)

class AtomicInteger(object):
    def __init__(self, initial=0):
        self.lock = threading.Lock()
        self.initial = initial

    def increment_and_get(self):
        with self.lock:
            self.initial += 1
            return self.initial
