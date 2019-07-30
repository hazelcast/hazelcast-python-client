from tests.base import HazelcastTestCase
from tests.util import configure_logging, random_string, event_collector, generate_key_owned_by_instance
from hazelcast.config import ClientConfig


class ListenerTest(HazelcastTestCase):
    # @classmethod
    # def setUpClass(cls):
    #     configure_logging()
    #     cls.rc = cls.create_rc()
    #     cls.cluster = cls.create_cluster(cls.rc, None)
    #
    # @classmethod
    # def tearDownClass(cls):
    #     cls.rc.exit()

    def setUp(self):
        configure_logging()
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc, None)
        self.m1 = self.cluster.start_member()
        self.m2 = self.cluster.start_member()
        self.m3 = self.cluster.start_member()
        self.client_config = ClientConfig()
        self.collector = event_collector()

    def tearDown(self):
        self.rc.exit()
        self.shutdown_all_clients()

    # -------------------------- test_remove_member ----------------------- #
    def test_smart_listener_remove_member(self):
        self.client_config.network_config.smart_routing = True
        client = self.create_client(self.client_config)
        map = client.get_map(random_string()).blocking()
        key_m1 = generate_key_owned_by_instance(client, self.m1.address)
        map.put(key_m1, 'value1')
        map.add_entry_listener(updated_func=self.collector)
        self.m1.shutdown()
        map.put(key_m1, 'value2')

        def assert_event():
            self.assertEqual(1, len(self.collector.events))
        self.assertTrueEventually(assert_event)

    def test_non_smart_listener_remove_partition_owner_member(self):
        self.client_config.network_config.smart_routing = False
        client = self.create_client(self.client_config)
        map = client.get_map(random_string()).blocking()
        key_m1 = generate_key_owned_by_instance(client, self.m1.address)
        map.put(key_m1, 'value1')
        map.add_entry_listener(updated_func=self.collector)
        self.m1.shutdown()
        map.put(key_m1, 'value2')
        event_count = len(self.collector.events)

        def assert_event():
            self.assertEqual(1, event_count)
        self.assertTrueEventually(assert_event)

    def test_non_smart_listener_remove_connection_owner_member(self):
        self.client_config.network_config.smart_routing = False
        client = self.create_client(self.client_config)
        map = client.get_map(random_string()).blocking()
        map.add_entry_listener(added_func=self.collector, updated_func=self.collector)

        owner_address = client.cluster.owner_connection_address
        generated_key = generate_key_owned_by_instance(client, owner_address)  # it might not be necessary to generate
        # key for this test. should i remove it to simplify the code?
        map.put(generated_key, 'value1')
        for m in [self.m1, self.m2, self.m3]:
            if m.address == owner_address:
                m.shutdown()
        map.put(generated_key, 'value2')

        def assert_event():
            self.assertEqual(2, len(self.collector.events))
        self.assertTrueEventually(assert_event)

    # -------------------------- test_add_member ----------------------- #
    def test_smart_listener_add_member(self):
        self.client_config.network_config.smart_routing = True
        client = self.create_client(self.client_config)
        map = client.get_map(random_string()).blocking()
        map.add_entry_listener(added_func=self.collector)
        m4 = self.cluster.start_member()
        key_m4 = generate_key_owned_by_instance(client, m4.address)
        map.put(key_m4, 'value')

        def assert_event():
            self.assertEqual(1, len(self.collector.events))
        self.assertTrueEventually(assert_event)

    def test_non_smart_listener_add_member(self):
        self.client_config.network_config.smart_routing = True
        client = self.create_client(self.client_config)
        map = client.get_map(random_string()).blocking()
        map.add_entry_listener(added_func=self.collector)
        m4 = self.cluster.start_member()
        key_m4 = generate_key_owned_by_instance(client, m4.address)
        map.put(key_m4, 'value')

        def assert_event():
            self.assertEqual(1, len(self.collector.events))
        self.assertTrueEventually(assert_event)

    # ------------------------------------------------------------------ #
    # Helpers:
    # def remove_member_test(self, client):
    #     map = client.get_map(random_string()).blocking()
    #     key_m1 = generate_key_owned_by_instance(client, self.m1.address)
    #     map.put(key_m1, 'value1')
    #     map.add_entry_listener(updated_func=self.collector)
    #     self.m1.shutdown()
    #     map.put(key_m1, 'value2')
    #     return len(self.collector.events)