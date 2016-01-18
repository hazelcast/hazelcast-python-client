import hazelcast
from tests.base import HazelcastTestCase


class ClusterTest(HazelcastTestCase):
    rc = None

    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc)

    def tearDown(self):
        self.shutdown_all_clients()
        self.rc.exit()

    def test_initial_membership_listener(self):
        events = []

        def member_added(m):
            events.append(m)

        config = hazelcast.ClientConfig()
        config.membership_listeners.append((member_added, None))

        member = self.cluster.start_member()

        self.create_client(config)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].uuid, member.uuid)
        self.assertEqual(events[0].address, member.address)

    def test_for_existing_members(self):
        events = []

        def member_added(member):
            events.append(member)

        member = self.cluster.start_member()
        client = self.create_client()

        client.cluster.add_listener(member_added, fire_for_existing=True)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].uuid, member.uuid)
        self.assertEqual(events[0].address, member.address)

    def test_member_added(self):
        events = []

        def member_added(member):
            events.append(member)

        self.cluster.start_member()
        client = self.create_client()

        client.cluster.add_listener(member_added, fire_for_existing=True)

        new_member = self.cluster.start_member()

        def assertion():
            self.assertEqual(len(events), 2)
            self.assertEqual(events[1].uuid, new_member.uuid)
            self.assertEqual(events[1].address, new_member.address)

        self.assertTrueEventually(assertion)

    def test_member_removed(self):
        events = []

        def member_removed(member):
            events.append(member)

        self.cluster.start_member()
        member_to_remove = self.cluster.start_member()

        client = self.create_client()

        client.cluster.add_listener(member_removed=member_removed)

        member_to_remove.shutdown()

        def assertion():
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].uuid, member_to_remove.uuid)
            self.assertEqual(events[0].address, member_to_remove.address)

        self.assertTrueEventually(assertion)

    def test_exception_in_membership_listener(self):
        def listener(e):
            raise RuntimeError("error")

        config = hazelcast.ClientConfig()
        config.membership_listeners.append((listener, listener))
        self.cluster.start_member()
        self.create_client(config)
