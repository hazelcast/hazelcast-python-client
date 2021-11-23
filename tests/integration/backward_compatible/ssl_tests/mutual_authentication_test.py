import os

import pytest

from tests.base import HazelcastTestCase
from hazelcast.client import HazelcastClient
from hazelcast.errors import HazelcastError
from tests.util import get_ssl_config, get_abs_path


@pytest.mark.enterprise
class MutualAuthenticationTest(HazelcastTestCase):
    current_directory = os.path.dirname(__file__)
    rc = None
    mutual_auth = True
    ma_req_xml = get_abs_path(current_directory, "hazelcast-ma-required.xml")
    ma_opt_xml = get_abs_path(current_directory, "hazelcast-ma-optional.xml")

    def setUp(self):
        self.rc = self.create_rc()

    def tearDown(self):
        self.rc.exit()

    def test_ma_required_client_and_server_authenticated(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(True))
        cluster.start_member()
        client = HazelcastClient(
            **get_ssl_config(
                cluster.id,
                True,
                get_abs_path(self.current_directory, "server1-cert.pem"),
                get_abs_path(self.current_directory, "client1-cert.pem"),
                get_abs_path(self.current_directory, "client1-key.pem"),
            )
        )
        self.assertTrue(client.lifecycle_service.is_running())
        client.shutdown()

    def test_ma_required_server_not_authenticated(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(True))
        cluster.start_member()

        with self.assertRaises(HazelcastError):
            HazelcastClient(
                **get_ssl_config(
                    cluster.id,
                    True,
                    get_abs_path(self.current_directory, "server2-cert.pem"),
                    get_abs_path(self.current_directory, "client1-cert.pem"),
                    get_abs_path(self.current_directory, "client1-key.pem"),
                )
            )

    def test_ma_required_client_not_authenticated(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(True))
        cluster.start_member()

        with self.assertRaises(HazelcastError):
            HazelcastClient(
                **get_ssl_config(
                    cluster.id,
                    True,
                    get_abs_path(self.current_directory, "server1-cert.pem"),
                    get_abs_path(self.current_directory, "client2-cert.pem"),
                    get_abs_path(self.current_directory, "client2-key.pem"),
                )
            )

    def test_ma_required_client_and_server_not_authenticated(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(True))
        cluster.start_member()

        with self.assertRaises(HazelcastError):
            HazelcastClient(
                **get_ssl_config(
                    cluster.id,
                    True,
                    get_abs_path(self.current_directory, "server2-cert.pem"),
                    get_abs_path(self.current_directory, "client2-cert.pem"),
                    get_abs_path(self.current_directory, "client2-key.pem"),
                )
            )

    def test_ma_optional_client_and_server_authenticated(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(False))
        cluster.start_member()
        client = HazelcastClient(
            **get_ssl_config(
                cluster.id,
                True,
                get_abs_path(self.current_directory, "server1-cert.pem"),
                get_abs_path(self.current_directory, "client1-cert.pem"),
                get_abs_path(self.current_directory, "client1-key.pem"),
            )
        )
        self.assertTrue(client.lifecycle_service.is_running())
        client.shutdown()

    def test_ma_optional_server_not_authenticated(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(False))
        cluster.start_member()

        with self.assertRaises(HazelcastError):
            HazelcastClient(
                **get_ssl_config(
                    cluster.id,
                    True,
                    get_abs_path(self.current_directory, "server2-cert.pem"),
                    get_abs_path(self.current_directory, "client1-cert.pem"),
                    get_abs_path(self.current_directory, "client1-key.pem"),
                )
            )

    def test_ma_optional_client_not_authenticated(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(False))
        cluster.start_member()

        with self.assertRaises(HazelcastError):
            HazelcastClient(
                **get_ssl_config(
                    cluster.id,
                    True,
                    get_abs_path(self.current_directory, "server1-cert.pem"),
                    get_abs_path(self.current_directory, "client2-cert.pem"),
                    get_abs_path(self.current_directory, "client2-key.pem"),
                )
            )

    def test_ma_optional_client_and_server_not_authenticated(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(False))
        cluster.start_member()

        with self.assertRaises(HazelcastError):
            HazelcastClient(
                **get_ssl_config(
                    cluster.id,
                    True,
                    get_abs_path(self.current_directory, "server2-cert.pem"),
                    get_abs_path(self.current_directory, "client2-cert.pem"),
                    get_abs_path(self.current_directory, "client2-key.pem"),
                )
            )

    def test_ma_required_with_no_cert_file(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(True))
        cluster.start_member()

        with self.assertRaises(HazelcastError):
            HazelcastClient(
                **get_ssl_config(
                    cluster.id, True, get_abs_path(self.current_directory, "server1-cert.pem")
                )
            )

    def test_ma_optional_with_no_cert_file(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(False))
        cluster.start_member()
        client = HazelcastClient(
            **get_ssl_config(
                cluster.id, True, get_abs_path(self.current_directory, "server1-cert.pem")
            )
        )
        self.assertTrue(client.lifecycle_service.is_running())
        client.shutdown()

    def configure_cluster(self, is_ma_required):
        file_path = self.ma_req_xml if is_ma_required else self.ma_opt_xml
        with open(file_path, "r") as f:
            return f.read()
