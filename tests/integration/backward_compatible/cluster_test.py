import os
import tempfile
import unittest

import pytest

from hazelcast import HazelcastClient
from hazelcast.util import RandomLB, RoundRobinLB
from tests.base import HazelcastTestCase, SingleMemberTestCase
from tests.util import (
    random_string,
    event_collector,
    skip_if_client_version_older_than,
    compare_client_version,
)

try:
    from hazelcast.core import EndpointQualifier, ProtocolType
except ImportError:
    # Added in 5.0 version of the client.
    pass


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
        config["membership_listeners"] = [(member_added, None)]

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
        config["membership_listeners"] = [(listener, listener)]
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

        self.assertEqual(
            0, len(client.cluster_service.get_members(lambda m: member.address != m.address))
        )


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

        self.assertCountEqual(
            self.addresses, list(map(lambda m: m.address, self._get_members_from_lb(lb)))
        )
        for _ in range(10):
            self.assertTrue(lb.next().address in self.addresses)

        client.shutdown()

    def test_round_robin_load_balancer(self):
        client = HazelcastClient(cluster_name=self.cluster.id, load_balancer=RoundRobinLB())
        self.assertTrue(client.lifecycle_service.is_running())

        lb = client._load_balancer
        self.assertTrue(isinstance(lb, RoundRobinLB))

        self.assertCountEqual(
            self.addresses, list(map(lambda m: m.address, self._get_members_from_lb(lb)))
        )
        for i in range(10):
            self.assertEqual(self.addresses[i % len(self.addresses)], lb.next().address)

        client.shutdown()

    @staticmethod
    def _get_members_from_lb(lb):
        # For backward-compatibility
        members = lb._members
        if isinstance(members, list):
            return members

        # 4.2+
        return members.members


@pytest.mark.enterprise
class HotRestartEventTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        tmp_dir = tempfile.gettempdir()
        cls.tmp_dir = os.path.join(tmp_dir, "hr-test-" + random_string())

    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster_keep_cluster_name(self.rc, self.get_config(5701))
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

        self.client.cluster_service.add_listener(
            member_added=added_listener, member_removed=removed_listener
        )

        self.rc.shutdownCluster(self.cluster.id)
        # now stop cluster, restart it with the same name and then start member with port 5702
        self.cluster = self.create_cluster_keep_cluster_name(self.rc, self.get_config(5702))
        self.cluster.start_member()

        def assertion():
            self.assertEqual(1, len(added_listener.events))
            self.assertEqual(1, len(removed_listener.events))

        self.assertTrueEventually(assertion)

        members = self.client.cluster_service.get_members()
        self.assertEqual(1, len(members))
        self.assertEqual(member.uuid, str(members[0].uuid))

    def test_when_member_started_with_the_same_address(self):
        skip_if_client_version_older_than(self, "4.2")

        old_member = self.cluster.start_member()
        self.client = HazelcastClient(cluster_name=self.cluster.id)

        members_added = []
        members_removed = []

        self.client.cluster_service.add_listener(
            lambda m: members_added.append(m), lambda m: members_removed.append(m)
        )

        self.rc.shutdownMember(self.cluster.id, old_member.uuid)
        new_member = self.cluster.start_member()

        def assertion():
            self.assertEqual(1, len(members_added))
            self.assertEqual(new_member.uuid, str(members_added[0].uuid))

            self.assertEqual(1, len(members_removed))
            self.assertEqual(old_member.uuid, str(members_removed[0].uuid))

        self.assertTrueEventually(assertion)

        members = self.client.cluster_service.get_members()
        self.assertEqual(1, len(members))
        self.assertEqual(new_member.uuid, str(members[0].uuid))

    def get_config(self, port):
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
        </hazelcast>""" % (
            port,
            self.tmp_dir,
        )


_SERVER_PORT = 5701
_CLIENT_PORT = 5702
_SERVER_WITH_CLIENT_ENDPOINT = """
<hazelcast xmlns="http://www.hazelcast.com/schema/config"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.hazelcast.com/schema/config
    http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
    <advanced-network enabled="true">
        <member-server-socket-endpoint-config>
            <port>%s</port>
        </member-server-socket-endpoint-config>
        <client-server-socket-endpoint-config>
            <port>%s</port>
        </client-server-socket-endpoint-config>
    </advanced-network>
</hazelcast>
""" % (
    _SERVER_PORT,
    _CLIENT_PORT,
)


@unittest.skipIf(
    compare_client_version("5.0") < 0, "Tests the features added in 5.0 version of the client"
)
class AdvancedNetworkConfigTest(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        return _SERVER_WITH_CLIENT_ENDPOINT

    @classmethod
    def configure_client(cls, config):
        config["cluster_members"] = ["localhost:%s" % _CLIENT_PORT]
        config["cluster_name"] = cls.cluster.id
        return config

    def test_member_list(self):
        members = self.client.cluster_service.get_members()
        self.assertEqual(1, len(members))
        member = members[0]

        # Make sure member address is assigned to client endpoint port
        self.assertEqual(_CLIENT_PORT, member.address.port)

        # Make sure there are mappings for CLIENT and MEMBER endpoints
        self.assertEqual(2, len(member.address_map))
        self.assertEqual(
            _SERVER_PORT, member.address_map.get(EndpointQualifier(ProtocolType.MEMBER, None)).port
        )
        self.assertEqual(
            _CLIENT_PORT,
            member.address_map.get(EndpointQualifier(ProtocolType.CLIENT, None)).port,
        )
