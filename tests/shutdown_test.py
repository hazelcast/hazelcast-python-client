from hazelcast import ClientConfig
from hazelcast.exception import HazelcastClientNotActiveException
from tests.base import HazelcastTestCase
from tests.util import configure_logging


class ShutdownTest(HazelcastTestCase):
    rc = None

    def setUp(self):
        configure_logging()
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc)

    def tearDown(self):
        self.shutdown_all_clients()
        self.rc.exit()

    def test_shutdown_not_hang_on_member_closed(self):
        config = ClientConfig()
        member = self.cluster.start_member()
        client = self.create_client(config)
        my_map = client.get_map("test")
        my_map.put("key", "value").result()
        member.shutdown()
        with self.assertRaises(HazelcastClientNotActiveException):
            while True:
                my_map.get("key").result()
