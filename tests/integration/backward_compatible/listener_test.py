import threading
import time

from parameterized import parameterized

from tests.base import HazelcastTestCase
from tests.util import (
    random_string,
    event_collector,
    generate_key_owned_by_instance,
    wait_for_partition_table,
)

LISTENER_TYPES = [
    (
        "smart",
        True,
    ),
    (
        "non-smart",
        False,
    ),
]


class ListenerRemoveMemberTest(HazelcastTestCase):
    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc, None)
        self.m1 = self.cluster.start_member()
        self.m2 = self.cluster.start_member()
        self.client_config = {
            "cluster_name": self.cluster.id,
            "heartbeat_interval": 1.0,
        }
        self.collector = event_collector()

    def tearDown(self):
        self.shutdown_all_clients()
        self.rc.terminateCluster(self.cluster.id)
        self.rc.exit()

    @parameterized.expand(LISTENER_TYPES)
    def test_remove_member(self, _, is_smart):
        self.client_config["smart_routing"] = is_smart
        client = self.create_client(self.client_config)
        wait_for_partition_table(client)
        key_m1 = generate_key_owned_by_instance(client, self.m1.uuid)
        random_map = client.get_map(random_string()).blocking()
        random_map.add_entry_listener(added_func=self.collector)
        self.m1.shutdown()
        random_map.put(key_m1, "value2")

        def assert_event():
            self.assertEqual(1, len(self.collector.events))

        self.assertTrueEventually(assert_event)


class ListenerAddMemberTest(HazelcastTestCase):
    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc, None)
        self.m1 = self.cluster.start_member()
        self.client_config = {
            "cluster_name": self.cluster.id,
        }
        self.collector = event_collector()

    def tearDown(self):
        self.shutdown_all_clients()
        self.rc.terminateCluster(self.cluster.id)
        self.rc.exit()

    @parameterized.expand(LISTENER_TYPES)
    def test_add_member(self, _, is_smart):
        self.client_config["smart_routing"] = is_smart
        client = self.create_client(self.client_config)
        random_map = client.get_map(random_string()).blocking()
        random_map.add_entry_listener(added_func=self.collector, updated_func=self.collector)
        m2 = self.cluster.start_member()
        wait_for_partition_table(client)
        key_m2 = generate_key_owned_by_instance(client, m2.uuid)

        assertion_succeeded = False

        def run():
            nonlocal assertion_succeeded
            # When a new connection is added, we add the existing
            # listeners to it, but we do it non-blocking. So, it might
            # be the case that, the listener registration request is
            # sent to the new member, but not completed yet.
            # So, we might not get an event for the put. To avoid this,
            # we will put multiple times.
            for i in range(10):
                if assertion_succeeded:
                    # We have successfully got an event
                    return

                random_map.put(key_m2, f"value-{i}")

                time.sleep((i + 1) * 0.1)

        put_thread = threading.Thread(target=run)
        put_thread.start()

        def assert_event():
            nonlocal assertion_succeeded
            self.assertGreaterEqual(len(self.collector.events), 1)
            assertion_succeeded = True

        self.assertTrueEventually(assert_event)
        put_thread.join()
