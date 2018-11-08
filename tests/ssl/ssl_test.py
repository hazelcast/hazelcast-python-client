import os

from tests.base import HazelcastTestCase
from hazelcast.client import HazelcastClient
from hazelcast.exception import HazelcastError
from hazelcast.config import PROTOCOL
from tests.util import get_ssl_config, configure_logging, fill_map, get_abs_path, set_attr


@set_attr(enterprise=True)
class SSLTest(HazelcastTestCase):
    current_directory = os.path.dirname(__file__)
    rc = None
    hazelcast_ssl_xml = get_abs_path(current_directory, "hazelcast-ssl.xml")

    @classmethod
    def setUpClass(cls):
        configure_logging()

    def setUp(self):
        self.rc = self.create_rc()
        cluster = self.create_cluster(self.rc, self.configure_cluster())
        member = cluster.start_member()

    def tearDown(self):
        self.rc.exit()

    def test_ssl_disabled(self):
        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(False))

    def test_ssl_enabled_is_client_live(self):
        client = HazelcastClient(get_ssl_config(True, get_abs_path(self.current_directory, "server1-cert.pem"),
                                                protocol=PROTOCOL.TLSv1))
        self.assertTrue(client.lifecycle.is_live)
        client.shutdown()

    def test_ssl_enabled_map_size(self):
        client = HazelcastClient(get_ssl_config(True, get_abs_path(self.current_directory, "server1-cert.pem"),
                                                protocol=PROTOCOL.TLSv1))
        test_map = client.get_map("test_map")
        fill_map(test_map, 10)
        self.assertEqual(test_map.size().result(), 10)
        client.shutdown()

    def test_ssl_enabled_with_custom_ciphers(self):
        client = HazelcastClient(get_ssl_config(True, get_abs_path(self.current_directory, "server1-cert.pem"),
                                                protocol=PROTOCOL.TLSv1,
                                                ciphers="DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA:DHE-RSA-DES-CBC3-SHA:DHE-RSA-DES-CBC3-SHA:DHE-DSS-DES-CBC3-SHA"))
        self.assertTrue(client.lifecycle.is_live)
        client.shutdown()

    def test_ssl_enabled_with_invalid_ciphers(self):
        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True,
                                                    get_abs_path(self.current_directory, "server1-cert.pem"),
                                                    protocol=PROTOCOL.TLSv1,
                                                    ciphers="INVALID-CIPHER1:INVALID_CIPHER2"))

    def test_ssl_enabled_with_protocol_mismatch(self):
        # member configured with TLSv1
        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True, get_abs_path(self.current_directory, "server1-cert.pem"),
                                                    protocol=PROTOCOL.SSLv3))

    def configure_cluster(self):
        with open(self.hazelcast_ssl_xml, "r") as f:
            return f.read()
