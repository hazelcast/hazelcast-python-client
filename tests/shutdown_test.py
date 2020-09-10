import threading

from hazelcast.errors import HazelcastClientNotActiveError
from tests.base import HazelcastTestCase


class ShutdownTest(HazelcastTestCase):
    rc = None

    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc)

    def tearDown(self):
        self.shutdown_all_clients()
        self.rc.terminateCluster(self.cluster.id)
        self.rc.exit()

    def test_shutdown_not_hang_on_member_closed(self):
        member = self.cluster.start_member()
        client = self.create_client({
            "cluster_name": self.cluster.id,
            "cluster_connect_timeout": 5.0,
        })
        my_map = client.get_map("test")
        my_map.put("key", "value").result()
        member.shutdown()
        with self.assertRaises(HazelcastClientNotActiveError):
            while True:
                my_map.get("key").result()

    def test_invocations_finalised_when_client_shutdowns(self):
        self.cluster.start_member()
        client = self.create_client({
            "cluster_name": self.cluster.id,
        })
        m = client.get_map("test")
        m.put("key", "value").result()

        def run():
            for _ in range(1000):
                try:
                    m.get("key").result()
                except:
                    pass

        threads = []
        for _ in range(10):
            t = threading.Thread(target=run)
            threads.append(t)
            t.start()

        client.shutdown()

        for i in range(10):
            threads[i].join(5)
