from tests.base import SingleMemberTestCase
from tests.util import random_string


class SemaphoreTest(SingleMemberTestCase):
    def setUp(self):
        self.semaphore = self.client.get_semaphore(random_string()).blocking()

    def test_init(self):
        self.semaphore.init(10)
        available_permits = self.semaphore.available_permits()
        self.assertEqual(available_permits, 10)

    def test_acquire(self):
        self.semaphore.init(10)
        self.semaphore.acquire(10)
        available_permits = self.semaphore.available_permits()
        self.assertEqual(available_permits, 0)

    def test_drain(self):
        self.semaphore.init(10)
        self.semaphore.drain_permits()
        available_permits = self.semaphore.available_permits()
        self.assertEqual(available_permits, 0)

    def test_reduce_permits(self):
        self.semaphore.init(10)
        self.semaphore.reduce_permits(10)
        available_permits = self.semaphore.available_permits()
        self.assertEqual(available_permits, 0)

    def test_release(self):
        self.semaphore.init(10)
        self.semaphore.acquire(10)
        self.semaphore.release(5)
        available_permits = self.semaphore.available_permits()
        self.assertEqual(available_permits, 5)

    def test_try_acquire(self):
        self.semaphore.init(10)
        self.semaphore.try_acquire(10)
        available_permits = self.semaphore.available_permits()
        self.assertEqual(available_permits, 0)

    def test_str(self):
        self.assertTrue(str(self.semaphore).startswith("Semaphore"))
