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
    default_ca_xml = get_abs_path(current_directory, "hazelcast-default-ca.xml")

    @classmethod
    def setUpClass(cls):
        configure_logging()

    def setUp(self):
        self.rc = self.create_rc()

    def tearDown(self):
        self.rc.exit()

    def test_ssl_disabled(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(self.hazelcast_ssl_xml))
        cluster.start_member()

        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(False))

    def test_ssl_enabled_is_client_live(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(self.hazelcast_ssl_xml))
        cluster.start_member()

        client = HazelcastClient(get_ssl_config(True, get_abs_path(self.current_directory, "server1-cert.pem"),
                                                protocol=PROTOCOL.TLSv1))
        self.assertTrue(client.lifecycle.is_live)
        client.shutdown()

    def test_ssl_enabled_trust_default_certificates(self):
        # Member started with Let's Encrypt certificate
        cluster = self.create_cluster(self.rc, self.configure_cluster(self.default_ca_xml))
        cluster.start_member()

        client = HazelcastClient(get_ssl_config(True, protocol=PROTOCOL.TLSv1))
        self.assertTrue(client.lifecycle.is_live)
        client.shutdown()

    def test_ssl_enabled_dont_trust_self_signed_certificates(self):
        # Member started with self-signed certificate
        cluster = self.create_cluster(self.rc, self.configure_cluster(self.hazelcast_ssl_xml))
        cluster.start_member()

        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True, protocol=PROTOCOL.TLSv1))

    def test_ssl_enabled_map_size(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(self.hazelcast_ssl_xml))
        cluster.start_member()

        client = HazelcastClient(get_ssl_config(True, get_abs_path(self.current_directory, "server1-cert.pem"),
                                                protocol=PROTOCOL.TLSv1))
        test_map = client.get_map("test_map")
        fill_map(test_map, 10)
        self.assertEqual(test_map.size().result(), 10)
        client.shutdown()

    def test_ssl_enabled_with_custom_ciphers(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(self.hazelcast_ssl_xml))
        cluster.start_member()

        client = HazelcastClient(get_ssl_config(True, get_abs_path(self.current_directory, "server1-cert.pem"),
                                                protocol=PROTOCOL.TLSv1,
                                                ciphers="DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA:DHE-RSA-DES-CBC3-SHA:DHE-RSA-DES-CBC3-SHA:DHE-DSS-DES-CBC3-SHA"))
        self.assertTrue(client.lifecycle.is_live)
        client.shutdown()

    def test_ssl_enabled_with_invalid_ciphers(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(self.hazelcast_ssl_xml))
        cluster.start_member()

        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True,
                                                    get_abs_path(self.current_directory, "server1-cert.pem"),
                                                    protocol=PROTOCOL.TLSv1,
                                                    ciphers="INVALID-CIPHER1:INVALID_CIPHER2"))

    def test_ssl_enabled_with_protocol_mismatch(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(self.hazelcast_ssl_xml))
        cluster.start_member()

        # Member configured with TLSv1
        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True, get_abs_path(self.current_directory, "server1-cert.pem"),
                                                    protocol=PROTOCOL.SSLv3))

    def configure_cluster(self, filename):
        with open(filename, "r") as f:
            return f.read()
