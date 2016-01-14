import unittest
import hazelcast
from tests.util import configure_logging


class ClusterTest(unittest.TestCase):
    def setUp(self):
        configure_logging()

    def test_initial_membership_listener(self):
        members = []

        def member_added(member):
            members.append(member)

        config = hazelcast.ClientConfig()
        config.membership_listeners.append((member_added, None))
        client = hazelcast.HazelcastClient(config)

        self.assertEqual(len(members), 1)
        self.assertEqual(members[0], client.cluster.members[0])
