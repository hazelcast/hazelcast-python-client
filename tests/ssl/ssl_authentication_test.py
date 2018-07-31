import os

from unittest import skipIf
from tests.base import HazelcastTestCase
from hazelcast.client import HazelcastClient
from hazelcast.exception import HazelcastError
from tests.util import is_oss, get_ssl_config, configure_logging, get_abs_path


@skipIf(is_oss(), "SSL/TLS is only supported with enterprise server.")
class SSLAuthenticationTest(HazelcastTestCase):
    current_directory = os.path.dirname(__file__)
    rc = None
    mutual_auth = True
    ma_req_xml = get_abs_path(current_directory, "hazelcast-ma-required.xml")
    ma_opt_xml = get_abs_path(current_directory, "hazelcast-ma-optional.xml")
    _hostname = "foo.bar.com"

    @classmethod
    def setUpClass(cls):
        configure_logging()

    def setUp(self):
        self.rc = self.create_rc()

    def tearDown(self):
        self.rc.exit()

    def test_mutual_authentication_required_with_valid_certificates(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(True))
        member = cluster.start_member()
        client = HazelcastClient(get_ssl_config(True,
                                                get_abs_path(self.current_directory, "server1-cert.pem"),
                                                get_abs_path(self.current_directory, "client1-cert.pem"),
                                                get_abs_path(self.current_directory, "client1-key.pem"),
                                                hostname=self._hostname))
        self.assertTrue(client.lifecycle.is_live)
        client.shutdown()

    def test_mutual_authentication_required_with_only_valid_client_certificate(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(True))
        member = cluster.start_member()

        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True,
                                                    get_abs_path(self.current_directory, "server2-cert.pem"),
                                                    get_abs_path(self.current_directory, "client1-cert.pem"),
                                                    get_abs_path(self.current_directory, "client1-key.pem"),
                                                    hostname=self._hostname))

    def test_mutual_authentication_required_with_only_valid_server_certificate(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(True))
        member = cluster.start_member()

        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True,
                                                    get_abs_path(self.current_directory, "server1-cert.pem"),
                                                    get_abs_path(self.current_directory, "client2-cert.pem"),
                                                    get_abs_path(self.current_directory, "client2-key.pem"),
                                                    hostname=self._hostname))

    def test_mutual_authentication_required_with_invalid_certificates(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(True))
        member = cluster.start_member()

        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True,
                                                    get_abs_path(self.current_directory, "server2-cert.pem"),
                                                    get_abs_path(self.current_directory, "client2-cert.pem"),
                                                    get_abs_path(self.current_directory, "client2-key.pem"),
                                                    hostname=self._hostname))

    def test_mutual_authentication_optional_with_valid_certificates(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(False))
        member = cluster.start_member()
        client = HazelcastClient(get_ssl_config(True,
                                                get_abs_path(self.current_directory, "server1-cert.pem"),
                                                get_abs_path(self.current_directory, "client1-cert.pem"),
                                                get_abs_path(self.current_directory, "client1-key.pem"),
                                                hostname=self._hostname))
        self.assertTrue(client.lifecycle.is_live)
        client.shutdown()

    def test_mutual_authentication_optional_with_only_valid_client_certificate(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(False))
        member = cluster.start_member()

        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True,
                                                    get_abs_path(self.current_directory, "server2-cert.pem"),
                                                    get_abs_path(self.current_directory, "client1-cert.pem"),
                                                    get_abs_path(self.current_directory, "client1-key.pem"),
                                                    hostname=self._hostname))

    def test_mutual_authentication_optional_with_only_valid_server_certificate(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(True))
        member = cluster.start_member()

        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True,
                                                    get_abs_path(self.current_directory, "server1-cert.pem"),
                                                    get_abs_path(self.current_directory, "client2-cert.pem"),
                                                    get_abs_path(self.current_directory, "client2-key.pem"),
                                                    hostname=self._hostname))

    def test_mutual_authentication_optional_with_invalid_certificates(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(True))
        member = cluster.start_member()

        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True,
                                                    get_abs_path(self.current_directory, "server2-cert.pem"),
                                                    get_abs_path(self.current_directory, "client2-cert.pem"),
                                                    get_abs_path(self.current_directory, "client2-key.pem"),
                                                    hostname=self._hostname))

    def test_mutual_authentication_required_with_no_client_certificates(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(True))
        member = cluster.start_member()

        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True, get_abs_path(self.current_directory, "server1-cert.pem")))

    def test_mutual_authentication_optional_with_no_client_certificates(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(False))
        member = cluster.start_member()
        client = HazelcastClient(get_ssl_config(True, get_abs_path(self.current_directory, "server1-cert.pem")))
        self.assertTrue(client.lifecycle.is_live)
        client.shutdown()

    def test_mutual_authentication_required_with_empty_server_name(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(True))
        member = cluster.start_member()

        client = HazelcastClient(get_ssl_config(True,
                                                get_abs_path(self.current_directory, "server1-cert.pem"),
                                                get_abs_path(self.current_directory, "client1-cert.pem"),
                                                get_abs_path(self.current_directory, "client1-key.pem")))
        self.assertTrue(client.lifecycle.is_live)
        client.shutdown()

    def test_mutual_authentication_optional_with_empty_server_name(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(False))
        member = cluster.start_member()

        client = HazelcastClient(get_ssl_config(True,
                                                get_abs_path(self.current_directory, "server1-cert.pem"),
                                                get_abs_path(self.current_directory, "client1-cert.pem"),
                                                get_abs_path(self.current_directory, "client1-key.pem")))
        self.assertTrue(client.lifecycle.is_live)
        client.shutdown()

    def test_mutual_authentication_required_with_invalid_server_name(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(True))
        member = cluster.start_member()

        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True,
                                                    get_abs_path(self.current_directory, "server1-cert.pem"),
                                                    get_abs_path(self.current_directory, "client1-cert.pem"),
                                                    get_abs_path(self.current_directory, "client1-key.pem"),
                                                    hostname="INVALID HOST NAME"))

    def test_mutual_authentication_optional_with_invalid_server_name(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(False))
        member = cluster.start_member()

        with self.assertRaises(HazelcastError):
            client = HazelcastClient(get_ssl_config(True,
                                                    get_abs_path(self.current_directory, "server1-cert.pem"),
                                                    get_abs_path(self.current_directory, "client1-cert.pem"),
                                                    get_abs_path(self.current_directory, "client1-key.pem"),
                                                    hostname="INVALID HOST NAME"))

    def configure_cluster(self, is_ma_required):
        file_path = self.ma_req_xml if is_ma_required else self.ma_opt_xml
        with open(file_path, "r") as f:
            return f.read()

