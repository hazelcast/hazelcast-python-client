import logging
import time

import hazelcast
from hazelcast.core import DistributedObjectEventType

from hazelcast.proxy import MAP_SERVICE
from tests.base import SingleMemberTestCase
from tests.util import event_collector, LoggingContext


class DistributedObjectsTest(SingleMemberTestCase):
    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc, cls.configure_cluster())
        cls.config = {"cluster_name": cls.cluster.id}

    @classmethod
    def tearDownClass(cls):
        cls.rc.exit()

    def setUp(self):
        self.member = self.cluster.start_member()
        self.client = hazelcast.HazelcastClient(**self.config)

    def tearDown(self):
        self.client.shutdown()
        self.member.shutdown()

    def test_get_distributed_objects(self):
        self.assertCountEqual(
            [], self._filter_internal_objects(self.client.get_distributed_objects())
        )

        m = self.client.get_map("map")
        s = self.client.get_set("set")
        q = self.client.get_queue("queue")

        self.assertTrueEventually(
            lambda: self.assertCountEqual(
                [m, s, q],
                self._filter_internal_objects(self.client.get_distributed_objects()),
            )
        )

    def test_get_distributed_objects_clears_destroyed_proxies(self):
        m = self.client.get_map("map")

        self.assertTrueEventually(
            lambda: self.assertCountEqual(
                [m], self._filter_internal_objects(self.client.get_distributed_objects())
            )
        )

        other_client = hazelcast.HazelcastClient(**self.config)
        other_clients_map = other_client.get_map("map")
        other_clients_map.destroy()

        self.assertTrueEventually(
            lambda: self.assertCountEqual(
                [], self._filter_internal_objects(self.client.get_distributed_objects())
            )
        )
        other_client.shutdown()

    def test_add_distributed_object_listener_object_created(self):
        collector = event_collector()
        self.client.add_distributed_object_listener(listener_func=collector).result()

        self.client.get_map("test-map")

        def assert_event():
            self.assertEqual(1, len(collector.events))
            event = collector.events[0]
            self.assertDistributedObjectEvent(
                event, "test-map", MAP_SERVICE, DistributedObjectEventType.CREATED
            )

        self.assertTrueEventually(assert_event)

    def test_add_distributed_object_listener_object_destroyed(self):
        with LoggingContext(logging.getLogger(), logging.DEBUG):
            collector = event_collector()
            m = self.client.get_map("test-map")
            self.client.add_distributed_object_listener(listener_func=collector).result()

            m.destroy()

            def assert_event():
                self.assertEqual(1, len(collector.events))
                event = collector.events[0]
                self.assertDistributedObjectEvent(
                    event, "test-map", MAP_SERVICE, DistributedObjectEventType.DESTROYED
                )

            self.assertTrueEventually(assert_event)

    def test_add_distributed_object_listener_object_created_and_destroyed(self):
        collector = event_collector()
        self.client.add_distributed_object_listener(listener_func=collector).result()

        m = self.client.get_map("test-map")
        m.destroy()

        def assert_event():
            self.assertEqual(2, len(collector.events))
            created_event = collector.events[0]
            destroyed_event = collector.events[1]
            self.assertDistributedObjectEvent(
                created_event, "test-map", MAP_SERVICE, DistributedObjectEventType.CREATED
            )
            self.assertDistributedObjectEvent(
                destroyed_event, "test-map", MAP_SERVICE, DistributedObjectEventType.DESTROYED
            )

        self.assertTrueEventually(assert_event)

    def test_remove_distributed_object_listener(self):
        collector = event_collector()
        reg_id = self.client.add_distributed_object_listener(listener_func=collector).result()
        m = self.client.get_map("test-map")

        def assert_event():
            self.assertEqual(1, len(collector.events))
            event = collector.events[0]
            self.assertDistributedObjectEvent(
                event, "test-map", MAP_SERVICE, DistributedObjectEventType.CREATED
            )

        self.assertTrueEventually(assert_event)

        response = self.client.remove_distributed_object_listener(reg_id).result()
        self.assertTrue(response)
        m.destroy()

        time.sleep(1)

        # We should only receive the map created event
        assert_event()

    def test_remove_invalid_distributed_object_listener(self):
        self.assertFalse(self.client.remove_distributed_object_listener("invalid-reg-id").result())

    @staticmethod
    def _filter_internal_objects(distributed_objects):
        return [obj for obj in distributed_objects if not obj.name.startswith("__")]
