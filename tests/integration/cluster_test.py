from hazelcast import HazelcastClient, six
from hazelcast.util import RandomLB, RoundRobinLB
from tests.base import HazelcastTestCase


class LoadBalancersWithRealClusterTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc, None)
        cls.member1 = cls.cluster.start_member()
        cls.member2 = cls.cluster.start_member()
        cls.addresses = [cls.member1.address, cls.member2.address]

    @classmethod
    def tearDownClass(cls):
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    def test_random_load_balancer(self):
        client = HazelcastClient(cluster_name=self.cluster.id, load_balancer=RandomLB())
        self.assertTrue(client.lifecycle_service.is_running())

        lb = client._load_balancer
        self.assertTrue(isinstance(lb, RandomLB))

        six.assertCountEqual(self, self.addresses, list(map(lambda m: m.address, lb._members)))
        for _ in range(10):
            self.assertTrue(lb.next().address in self.addresses)

        client.shutdown()

    def test_round_robin_load_balancer(self):
        client = HazelcastClient(cluster_name=self.cluster.id, load_balancer=RoundRobinLB())
        self.assertTrue(client.lifecycle_service.is_running())

        lb = client._load_balancer
        self.assertTrue(isinstance(lb, RoundRobinLB))

        six.assertCountEqual(self, self.addresses, list(map(lambda m: m.address, lb._members)))
        for i in range(10):
            self.assertEqual(self.addresses[i % len(self.addresses)], lb.next().address)

        client.shutdown()
