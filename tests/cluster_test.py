import unittest

from hzrc.client import HzRemoteController

import hazelcast
from tests.base import SingleMemberTestCase, HazelcastTestCase
from tests.util import configure_logging


class ClusterTest(HazelcastTestCase):
    rc = None

    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc)

    def tearDown(self):
        self.rc.exit()

    def test_initial_membership_listener(self):
        members = []

        def member_added(member):
            members.append(member)

        config = hazelcast.ClientConfig()
        config.membership_listeners.append((member_added, None))

        self.cluster.start_member()
        client = hazelcast.HazelcastClient(config)

        self.assertEqual(len(members), 1)
        self.assertEqual(members[0], client.cluster.members[0])
