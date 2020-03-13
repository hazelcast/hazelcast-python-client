import hazelcast
from hazelcast.core import DistributedObjectEventType

from hazelcast.proxy import MAP_SERVICE
from tests.base import SingleMemberTestCase
from tests.util import event_collector
from hazelcast import six


class DistributedObjectsTest(SingleMemberTestCase):
    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc, cls.configure_cluster())

    @classmethod
    def tearDownClass(cls):
        cls.rc.exit()

    def setUp(self):
        self.member = self.cluster.start_member()
        self.client = hazelcast.HazelcastClient()

    def tearDown(self):
        self.client.shutdown()
        self.member.shutdown()

    def test_get_distributed_objects(self):
        six.assertCountEqual(self, [], self.client.get_distributed_objects())

        m = self.client.get_map("map")
        s = self.client.get_set("set")
        q = self.client.get_queue("queue")

        six.assertCountEqual(self, [m, s, q], self.client.get_distributed_objects())

    def test_get_distributed_objects_clears_destroyed_proxies(self):
        m = self.client.get_map("map")

        six.assertCountEqual(self, [m], self.client.get_distributed_objects())

        other_client = hazelcast.HazelcastClient()
        other_clients_map = other_client.get_map("map")
        other_clients_map.destroy()

        six.assertCountEqual(self, [], self.client.get_distributed_objects())

    def test_add_distributed_object_listener_object_created(self):
        collector = event_collector()
        self.client.add_distributed_object_listener(listener_func=collector)

        self.client.get_map("test-map")

        def assert_event():
            self.assertEqual(1, len(collector.events))
            event = collector.events[0]
            self.assertDistributedObjectEvent(event, "test-map", MAP_SERVICE, DistributedObjectEventType.CREATED)

        self.assertTrueEventually(assert_event)

    def test_add_distributed_object_listener_object_destroyed(self):
        collector = event_collector()
        m = self.client.get_map("test-map")
        self.client.add_distributed_object_listener(listener_func=collector)

        m.destroy()

        def assert_event():
            self.assertEqual(1, len(collector.events))
            event = collector.events[0]
            self.assertDistributedObjectEvent(event, "test-map", MAP_SERVICE, DistributedObjectEventType.DESTROYED)

        self.assertTrueEventually(assert_event)

    def test_add_distributed_object_listener_object_created_and_destroyed(self):
        collector = event_collector()
        self.client.add_distributed_object_listener(listener_func=collector)

        m = self.client.get_map("test-map")
        m.destroy()

        def assert_event():
            self.assertEqual(2, len(collector.events))
            created_event = collector.events[0]
            destroyed_event = collector.events[1]
            self.assertDistributedObjectEvent(created_event, "test-map", MAP_SERVICE,
                                              DistributedObjectEventType.CREATED)
            self.assertDistributedObjectEvent(destroyed_event, "test-map", MAP_SERVICE,
                                              DistributedObjectEventType.DESTROYED)

        self.assertTrueEventually(assert_event)

    def test_remove_distributed_object_listener(self):
        collector = event_collector()
        reg_id = self.client.add_distributed_object_listener(listener_func=collector)
        m = self.client.get_map("test-map")

        response = self.client.remove_distributed_object_listener(reg_id)
        self.assertTrue(response)
        m.destroy()

        # only map creation should be notified
        def assert_event():
            self.assertEqual(1, len(collector.events))
            event = collector.events[0]
            self.assertDistributedObjectEvent(event, "test-map", MAP_SERVICE, DistributedObjectEventType.CREATED)
        self.assertTrueEventually(assert_event)

    def test_remove_invalid_distributed_object_listener(self):
        self.assertFalse(self.client.remove_distributed_object_listener("invalid-reg-id"))
