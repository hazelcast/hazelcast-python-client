import unittest

from hazelcast.util import RandomLB, RoundRobinLB


class _MockMember(object):
    def __init__(self, id, lite_member):
        self.id = id
        self.lite_member = lite_member


class _MockClusterService(object):
    def __init__(self, data_member_count, lite_member_count):
        id = 0
        data_members = []
        lite_members = []

        for _ in range(data_member_count):
            data_members.append(_MockMember(id, False))
            id += 1

        for _ in range(lite_member_count):
            lite_members.append(_MockMember(id, True))
            id += 1

        self._members = data_members + lite_members

    def add_listener(self, listener, *_):
        for m in self._members:
            listener(m)

    def get_members(self):
        return self._members


class LoadBalancersTest(unittest.TestCase):
    def test_random_lb_with_no_members(self):
        cluster = _MockClusterService(0, 0)
        lb = RandomLB()
        lb.init(cluster)
        self.assertIsNone(lb.next())
        self.assertIsNone(lb.next_data_member())

    def test_round_robin_lb_with_no_members(self):
        cluster = _MockClusterService(0, 0)
        lb = RoundRobinLB()
        lb.init(cluster)
        self.assertIsNone(lb.next())
        self.assertIsNone(lb.next_data_member())

    def test_random_lb_with_data_members(self):
        cluster = _MockClusterService(3, 0)
        lb = RandomLB()
        lb.init(cluster)
        self.assertTrue(lb.can_get_next_data_member())
        for _ in range(10):
            self.assertTrue(0 <= lb.next().id <= 2)
            self.assertTrue(0 <= lb.next_data_member().id <= 2)

    def test_round_robin_lb_with_data_members(self):
        cluster = _MockClusterService(5, 0)
        lb = RoundRobinLB()
        lb.init(cluster)
        self.assertTrue(lb.can_get_next_data_member())

        for i in range(10):
            self.assertEqual(i % 5, lb.next().id)

        for i in range(10):
            self.assertEqual(i % 5, lb.next_data_member().id)

    def test_random_lb_with_lite_members(self):
        cluster = _MockClusterService(0, 3)
        lb = RandomLB()
        lb.init(cluster)

        for _ in range(10):
            self.assertTrue(0 <= lb.next().id <= 2)
            self.assertIsNone(lb.next_data_member())

    def test_round_robin_lb_with_lite_members(self):
        cluster = _MockClusterService(0, 3)
        lb = RoundRobinLB()
        lb.init(cluster)

        for i in range(10):
            self.assertEqual(i % 3, lb.next().id)
            self.assertIsNone(lb.next_data_member())

    def test_random_lb_with_mixed_members(self):
        cluster = _MockClusterService(3, 3)
        lb = RandomLB()
        lb.init(cluster)

        for _ in range(20):
            self.assertTrue(0 <= lb.next().id < 6)
            self.assertTrue(0 <= lb.next_data_member().id < 3)

    def test_round_robin_lb_with_mixed_members(self):
        cluster = _MockClusterService(3, 3)
        lb = RoundRobinLB()
        lb.init(cluster)

        for i in range(24):
            self.assertEqual(i % 6, lb.next().id)

        for i in range(24):
            self.assertEqual(i % 3, lb.next_data_member().id)
