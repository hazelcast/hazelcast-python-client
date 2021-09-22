from tests.base import HazelcastTestCase
from tests.util import random_string, event_collector
from time import sleep


class SmartListenerTest(HazelcastTestCase):

    rc = None
    cluster = None

    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc, None)
        cls.cluster.start_member()
        cls.cluster.start_member()

    @classmethod
    def tearDownClass(cls):
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    def setUp(self):
        self.client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "smart_routing": True,
            }
        )
        self.collector = event_collector()

    def tearDown(self):
        self.shutdown_all_clients()

    def test_list_smart_listener_local_only(self):
        list = self.client.get_list(random_string()).blocking()
        list.add_listener(item_added_func=self.collector)
        list.add("item-value")
        self.assert_event_received_once()

    def test_map_smart_listener_local_only(self):
        map = self.client.get_map(random_string()).blocking()
        map.add_entry_listener(added_func=self.collector)
        map.put("key", "value")
        self.assert_event_received_once()

    def test_multimap_smart_listener_local_only(self):
        multimap = self.client.get_map(random_string()).blocking()
        multimap.add_entry_listener(added_func=self.collector)
        multimap.put("key", "value")
        self.assert_event_received_once()

    def test_queue_smart_listener_local_only(self):
        queue = self.client.get_queue(random_string()).blocking()
        queue.add_listener(item_added_func=self.collector)
        queue.add("item-value")
        self.assert_event_received_once()

    def test_replicated_map_smart_listener_local_only(self):
        replicated_map = self.client.get_replicated_map(random_string()).blocking()
        replicated_map.add_entry_listener(added_func=self.collector)
        replicated_map.put("key", "value")
        self.assert_event_received_once()

    def test_set_smart_listener_local_only(self):
        set = self.client.get_set(random_string()).blocking()
        set.add_listener(item_added_func=self.collector)
        set.add("item-value")
        self.assert_event_received_once()

    def test_topic_smart_listener_local_only(self):
        topic = self.client.get_topic(random_string()).blocking()
        topic.add_listener(on_message=self.collector)
        topic.publish("item-value")
        self.assert_event_received_once()

    def assert_event_received_once(self):
        sleep(2)
        self.assertEqual(1, len(self.collector.events))
