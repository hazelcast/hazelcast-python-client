import os

from unittest import skipIf
from tests.base import HazelcastTestCase
from hazelcast.client import HazelcastClient
from hazelcast.exception import HazelcastError
from tests.util import is_oss, get_ssl_config, configure_logging, fill_map, get_abs_path


@skipIf(is_oss(), "SSL/TLS is only supported with enterprise server.")
class SSLTest(HazelcastTestCase):
    current_directory = os.path.dirname(__file__)
    rc = None
    hazelcast_xml = get_abs_path(current_directory, "hazelcast-ssl.xml")

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
        client = HazelcastClient(get_ssl_config(True, get_abs_path(self.current_directory, "server1-cert.pem")))
        self.assertTrue(client.lifecycle.is_live)
        client.shutdown()

    def test_ssl_enabled_map_size(self):
        client = HazelcastClient(get_ssl_config(True, get_abs_path(self.current_directory, "server1-cert.pem")))
        test_map = client.get_map("test_map")
        fill_map(test_map, 10)
        self.assertEqual(test_map.size().result(), 10)
        client.shutdown()

    def test_ssl_enabled_with_hostname(self):
        client = HazelcastClient(get_ssl_config(True,
                                                get_abs_path(self.current_directory, "server1-cert.pem"),
                                                hostname="foo.bar.com"))
        self.assertTrue(client.lifecycle.is_live)
        client.shutdown()

    def test_ssl_enabled_with_invalid_hostname(self):
        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True,
                                                    get_abs_path(self.current_directory, "server1-cert.pem"),
                                                    hostname="INVALID HOST NAME"))

    def test_ssl_enabled_with_unknown_ciphers(self):
        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True,
                                                    get_abs_path(self.current_directory, "server1-cert.pem"),
                                                    ciphers="UNKNOWN CIPHERS"))

    def configure_cluster(self):
        with open(self.hazelcast_xml, "r") as f:
            return f.read()
