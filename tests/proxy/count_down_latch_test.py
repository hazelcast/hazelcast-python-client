import threading
from time import sleep

from tests.base import SingleMemberTestCase
from tests.util import random_string


class CountDownLatchTestCase(SingleMemberTestCase):
    def setUp(self):
        self.latch = self.client.get_count_down_latch(random_string()).blocking()

    def test_latch(self):
        self.latch.try_set_count(20)

        self.assertEqual(self.latch.get_count(), 20)

        def test_run():
            for i in xrange(0, 20):
                self.latch.count_down()
                sleep(0.06)

        _thread = threading.Thread(target=test_run)
        _thread.start()

        self.assertFalse(self.latch.await(1))
        self.assertTrue(self.latch.await(15))

    def test_str(self):
        str_ = self.latch.__str__()
        self.assertTrue(str_.startswith("CountDownLatch"))
