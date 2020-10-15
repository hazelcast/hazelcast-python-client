import os
import tempfile
import unittest

from hazelcast import HazelcastClient, six
from hazelcast.util import RandomLB, RoundRobinLB
from tests.base import HazelcastTestCase
from tests.util import set_attr, random_string, event_collector


class ClusterTest(HazelcastTestCase):
    rc = None

    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc)

    def create_config(self):
        return {
            "cluster_name": self.cluster.id,
        }

    def tearDown(self):
        self.shutdown_all_clients()
        self.rc.terminateCluster(self.cluster.id)
        self.rc.exit()

    def test_initial_membership_listener(self):
        events = []

        def member_added(m):
            events.append(m)

        config = self.create_config()
        config["membership_listeners"] = [
            (member_added, None)
        ]

        member = self.cluster.start_member()

        self.create_client(config)

        self.assertEqual(len(events), 1)
        self.assertEqual(str(events[0].uuid), member.uuid)
        self.assertEqual(events[0].address, member.address)

    def test_for_existing_members(self):
        events = []

        def member_added(member):
            events.append(member)

        member = self.cluster.start_member()
        config = self.create_config()
        client = self.create_client(config)

        client.cluster_service.add_listener(member_added, fire_for_existing=True)

        self.assertEqual(len(events), 1)
        self.assertEqual(str(events[0].uuid), member.uuid)
        self.assertEqual(events[0].address, member.address)

    def test_member_added(self):
        events = []

        def member_added(member):
            events.append(member)

        self.cluster.start_member()
        config = self.create_config()
        client = self.create_client(config)

        client.cluster_service.add_listener(member_added, fire_for_existing=True)

        new_member = self.cluster.start_member()

        def assertion():
            self.assertEqual(len(events), 2)
            self.assertEqual(str(events[1].uuid), new_member.uuid)
            self.assertEqual(events[1].address, new_member.address)

        self.assertTrueEventually(assertion)

    def test_member_removed(self):
        events = []

        def member_removed(member):
            events.append(member)

        self.cluster.start_member()
        member_to_remove = self.cluster.start_member()

        config = self.create_config()
        client = self.create_client(config)

        client.cluster_service.add_listener(member_removed=member_removed)

        member_to_remove.shutdown()

        def assertion():
            self.assertEqual(len(events), 1)
            self.assertEqual(str(events[0].uuid), member_to_remove.uuid)
            self.assertEqual(events[0].address, member_to_remove.address)

        self.assertTrueEventually(assertion)

    def test_exception_in_membership_listener(self):
        def listener(_):
            raise RuntimeError("error")

        config = self.create_config()
        config["membership_listeners"] = [
            (listener, listener)
        ]
        self.cluster.start_member()
        self.create_client(config)

    def test_cluster_service_get_members(self):
        self.cluster.start_member()
        config = self.create_config()
        client = self.create_client(config)

        self.assertEqual(1, len(client.cluster_service.get_members()))

    def test_cluster_service_get_members_with_selector(self):
        member = self.cluster.start_member()
        config = self.create_config()
        client = self.create_client(config)

        self.assertEqual(0, len(client.cluster_service.get_members(lambda m: member.address != m.address)))


class _MockClusterService(object):
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


@set_attr(enterprise=True)
class HotRestartEventTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        tmp_dir = tempfile.gettempdir()
        cls.tmp_dir = os.path.join(tmp_dir, "hr-test-" + random_string())

    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster_keep_cluster_name(self.rc, self._get_config(5701))
        self.client = None

    def tearDown(self):
        if self.client:
            self.client.shutdown()

        self.rc.terminateCluster(self.cluster.id)
        self.rc.exit()

    def test_when_member_started_with_another_port_and_the_same_uuid(self):
        member = self.cluster.start_member()
        self.client = HazelcastClient(cluster_name=self.cluster.id)

        added_listener = event_collector()
        removed_listener = event_collector()

        self.client.cluster_service.add_listener(member_added=added_listener, member_removed=removed_listener)

        self.rc.shutdownCluster(self.cluster.id)
        # now stop cluster, restart it with the same name and then start member with port 5702
        self.cluster = self.create_cluster_keep_cluster_name(self.rc, self._get_config(5702))
        self.cluster.start_member()

        def assertion():
            self.assertEqual(1, len(added_listener.events))
            self.assertEqual(1, len(removed_listener.events))

        self.assertTrueEventually(assertion)

        members = self.client.cluster_service.get_members()
        self.assertEqual(1, len(members))
        self.assertEqual(member.uuid, str(members[0].uuid))

    def _get_config(self, port):
        return """
        <hazelcast xmlns="http://www.hazelcast.com/schema/config"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.hazelcast.com/schema/config
            http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
            <cluster-name>hot-restart-test</cluster-name>
            <network>
               <port>%s</port>
            </network>
            <hot-restart-persistence enabled="true">
                <base-dir>%s</base-dir>
            </hot-restart-persistence>
        </hazelcast>""" % (port, self.tmp_dir)
