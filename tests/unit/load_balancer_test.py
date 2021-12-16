import unittest

from hazelcast.util import RandomLB, RoundRobinLB


class _MockClusterService:
    def __init__(self, members):
        self._members = members

    def add_listener(self, listener, *_):
        for m in self._members:
            listener(m)

    def get_members(self):
        return self._members


class LoadBalancersTest(unittest.TestCase):
    def test_random_lb_with_no_members(self):
        cluster = _MockClusterService([])
        lb = RandomLB()
        lb.init(cluster)
        self.assertIsNone(lb.next())

    def test_round_robin_lb_with_no_members(self):
        cluster = _MockClusterService([])
        lb = RoundRobinLB()
        lb.init(cluster)
        self.assertIsNone(lb.next())

    def test_random_lb_with_members(self):
        cluster = _MockClusterService([0, 1, 2])
        lb = RandomLB()
        lb.init(cluster)
        for _ in range(10):
            self.assertTrue(0 <= lb.next() <= 2)

    def test_round_robin_lb_with_members(self):
        cluster = _MockClusterService([0, 1, 2])
        lb = RoundRobinLB()
        lb.init(cluster)
        for i in range(10):
            self.assertEqual(i % 3, lb.next())
