from tests.base import HazelcastTestCase
from tests.util import random_string, event_collector, generate_key_owned_by_instance, wait_for_partition_table


class ListenerTest(HazelcastTestCase):
    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc, None)
        self.m1 = self.cluster.start_member()
        self.m2 = self.cluster.start_member()
        self.client_config = {
            "cluster_name": self.cluster.id,
        }
        self.collector = event_collector()

    def tearDown(self):
        self.shutdown_all_clients()
        self.rc.exit()

    # -------------------------- test_remove_member ----------------------- #
    def test_smart_listener_remove_member(self):
        self.client_config["smart_routing"] = True
        client = self.create_client(self.client_config)
        wait_for_partition_table(client)
        key_m1 = generate_key_owned_by_instance(client, self.m1.uuid)
        map = client.get_map(random_string()).blocking()
        map.put(key_m1, 'value1')
        map.add_entry_listener(updated_func=self.collector)
        self.m1.shutdown()
        map.put(key_m1, 'value2')

        def assert_event():
            self.assertEqual(1, len(self.collector.events))
        self.assertTrueEventually(assert_event)

    def test_non_smart_listener_remove_member(self):
        self.client_config["smart_routing"] = False
        client = self.create_client(self.client_config)
        map = client.get_map(random_string()).blocking()
        map.add_entry_listener(added_func=self.collector)
        self.m2.shutdown()
        wait_for_partition_table(client)

        generated_key = generate_key_owned_by_instance(client, self.m1.uuid)
        map.put(generated_key, 'value')

        def assert_event():
            self.assertEqual(1, len(self.collector.events))
        self.assertTrueEventually(assert_event)

    # -------------------------- test_add_member ----------------------- #
    def test_smart_listener_add_member(self):
        self.client_config["smart_routing"] = True
        client = self.create_client(self.client_config)
        map = client.get_map(random_string()).blocking()
        map.add_entry_listener(added_func=self.collector)
        m3 = self.cluster.start_member()
        wait_for_partition_table(client)
        key_m3 = generate_key_owned_by_instance(client, m3.uuid)
        map.put(key_m3, 'value')

        def assert_event():
            self.assertEqual(1, len(self.collector.events))
        self.assertTrueEventually(assert_event)

    def test_non_smart_listener_add_member(self):
        self.client_config["smart_routing"] = False
        client = self.create_client(self.client_config)
        map = client.get_map(random_string()).blocking()
        map.add_entry_listener(added_func=self.collector)
        m3 = self.cluster.start_member()
        wait_for_partition_table(client)
        key_m3 = generate_key_owned_by_instance(client, m3.uuid)
        map.put(key_m3, 'value')

        def assert_event():
            self.assertEqual(1, len(self.collector.events))
        self.assertTrueEventually(assert_event)
