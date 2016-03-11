import sys
import threading
from random import choice
from unittest import skip

from nose.plugins.attrib import attr

from tests.base import SingleMemberTestCase
from tests.util import random_string


class ThreadingTest(SingleMemberTestCase):
    def setUp(self):
        self.map = self.client.get_map(random_string()).blocking()


    @attr("stress_test")
    @skip
    def test_operation_from_multiple_threads(self):
        num_threads = 4
        num_iterations = 5000
        value_size = 1000
        key_range = 50
        timeout = 300

        keys = range(0, key_range)

        exceptions = []
        value = "v" * value_size

        def put_get_remove():
            for i in xrange(0, num_iterations):
                if i % 100 == 0:
                    self.logger.info("op %i", i)
                try:
                    key = choice(keys)
                    self.map.lock(key)
                    self.map.put(key, value)
                    self.assertEqual(value, self.map.get(key))
                    self.assertEqual(value, self.map.remove(key))
                    self.map.unlock(key)
                except:
                    self.logger.exception("Exception in thread")
                    exceptions.append((threading.currentThread().getName(), sys.exc_info()))

        threads = [self.start_new_thread(put_get_remove) for _ in xrange(0, num_threads)]

        for t in threads:
            t.join(timeout)
            if t.isAlive():
                self.fail("thread %s did not finish in %s seconds" % (t.getName(), timeout))

        if exceptions:
            name, exception = exceptions[0]
            self.logger.exception("Exception in thread %s", name)
            raise exception[0], None, exception[2]
