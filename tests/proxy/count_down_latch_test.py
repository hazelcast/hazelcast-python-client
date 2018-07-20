import threading
from time import sleep

from tests.base import SingleMemberTestCase
from tests.util import random_string
from hazelcast import six
from hazelcast.six.moves import range


class CountDownLatchTest(SingleMemberTestCase):
    def setUp(self):
        self.latch = self.client.get_count_down_latch(random_string()).blocking()

    def test_latch(self):
        self.latch.try_set_count(20)

        self.assertEqual(self.latch.get_count(), 20)

        def test_run():
            for i in range(0, 20):
                self.latch.count_down()
                sleep(0.06)

        _thread = threading.Thread(target=test_run)
        _thread.start()

        if six.PY2:
            six.exec_("""self.assertFalse(self.latch.await(1))""")
            six.exec_("""self.assertTrue(self.latch.await(15))""")
        else:
            self.assertFalse(self.latch.await_latch(1))
            self.assertTrue(self.latch.await_latch(15))

    def test_str(self):
        self.assertTrue(str(self.latch).startswith("CountDownLatch"))
