from tests.base import HazelcastTestCase
from tests.util import configure_logging, random_string, event_collector
from hazelcast.config import ClientConfig
from time import sleep


class SmartListenerTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        configure_logging()
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc, None)  # Default config
        cls.m1 = cls.cluster.start_member()
        cls.m2 = cls.cluster.start_member()
        cls.m3 = cls.cluster.start_member()

    @classmethod
    def tearDownClass(cls):
        cls.rc.exit()

    def setUp(self):
        client_config = ClientConfig()
        client_config.network_config.smart_routing = True
        self.client = self.create_client(client_config)
        self.collector = event_collector()

    def tearDown(self):
        self.shutdown_all_clients()

    # -------------------------- test_local_only ----------------------- #
    def test_list_smart_listener_local_only(self):
        list = self.client.get_list(random_string()).blocking()
        list.add_listener(item_added_func=self.collector)
        list.add('item-value')
        sleep(5)
        self.assertEqual(1, len(self.collector.events))

    def test_map_smart_listener_local_only(self):
        map = self.client.get_map(random_string()).blocking()
        map.add_entry_listener(added_func=self.collector)
        map.put('key', 'value')
        sleep(5)
        self.assertEqual(1, len(self.collector.events))

    def test_multimap_smart_listener_local_only(self):
        multimap = self.client.get_map(random_string()).blocking()
        multimap.add_entry_listener(added_func=self.collector)
        multimap.put('key', 'value')
        sleep(5)
        self.assertEqual(1, len(self.collector.events))

    def test_queue_smart_listener_local_only(self):
        queue = self.client.get_queue(random_string()).blocking()
        queue.add_listener(item_added_func=self.collector)
        queue.add('item-value')
        sleep(5)
        self.assertEqual(1, len(self.collector.events))

    def test_replicated_map_smart_listener_local_only(self):
        replicated_map = self.client.get_replicated_map(random_string()).blocking()
        replicated_map.add_entry_listener(added_func=self.collector)
        replicated_map.put('key', 'value')
        sleep(5)
        self.assertEqual(1, len(self.collector.events))

    def test_set_smart_listener_local_only(self):
        set = self.client.get_set(random_string()).blocking()
        set.add_listener(item_added_func=self.collector)
        set.add('item-value')
        sleep(5)
        self.assertEqual(1, len(self.collector.events))

    def test_topic_smart_listener_local_only(self):
        topic = self.client.get_topic(random_string()).blocking()
        topic.add_listener(on_message=self.collector)
        topic.publish('item-value')
        sleep(5)
        self.assertEqual(1, len(self.collector.events))

    """
    def test_non_smart_listeners_terminate_random_node(self):
        pass
        cluster = self.create_cluster(self.rc)
        for i in range(3):
            cluster.start_member()
        client_config = self.get_non_smart_client_config()
        self.create_client(client_config)
        self.listeners_terminate_random_node()

    def test_smart_listeners_terminate_random_node(self):
        pass
        cluster = self.create_cluster(self.rc)
        for i in range(3):
            cluster.start_member()
        client_config = self.get_smart_client_config()
        self.create_client(client_config)
        self.listeners_terminate_random_node()

    def listeners_terminate_random_node(self):
        pass

    # Helpers:
    def set_up_listener(self):
        pass

    def get_smart_client_config(self):
        client_config = ClientConfig()
        client_config.network_config.connection_attempt_limit = MAXSIZE
        client_config.network_config.redo_operation = True  # Isn't this dangerous for non-idempotent operations?
        return client_config

    def get_non_smart_client_config(self):
        client_config = self.get_smart_client_config()
        client_config.network_config.smart_routing = False
        return client_config
    """
