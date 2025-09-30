import os
import unittest

import pytest

from tests.integration.asyncio.base import HazelcastTestCase
from hazelcast.asyncio.client import HazelcastClient
from hazelcast.errors import HazelcastError
from tests.util import get_ssl_config, get_abs_path


@pytest.mark.enterprise
class MutualAuthenticationTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
    current_directory = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../backward_compatible/ssl_tests")
    )
    rc = None
    mutual_auth = True
    ma_req_xml = get_abs_path(current_directory, "hazelcast-ma-required.xml")
    ma_opt_xml = get_abs_path(current_directory, "hazelcast-ma-optional.xml")

    def setUp(self):
        self.rc = self.create_rc()

    def tearDown(self):
        self.rc.exit()

    async def test_ma_required_client_and_server_authenticated(self):
        cluster = self.create_cluster(self.rc, self.read_config(True))
        cluster.start_member()
        client = await HazelcastClient.create_and_start(
            **get_ssl_config(
                cluster.id,
                True,
                get_abs_path(self.current_directory, "server1-cert.pem"),
                get_abs_path(self.current_directory, "client1-cert.pem"),
                get_abs_path(self.current_directory, "client1-key.pem"),
            )
        )
        self.assertTrue(client.lifecycle_service.is_running())
        await client.shutdown()

    async def test_ma_required_server_not_authenticated(self):
        cluster = self.create_cluster(self.rc, self.read_config(True))
        cluster.start_member()
        with self.assertRaises(HazelcastError):
            await HazelcastClient.create_and_start(
                **get_ssl_config(
                    cluster.id,
                    True,
                    get_abs_path(self.current_directory, "server2-cert.pem"),
                    get_abs_path(self.current_directory, "client1-cert.pem"),
                    get_abs_path(self.current_directory, "client1-key.pem"),
                )
            )

    async def test_ma_required_client_not_authenticated(self):
        cluster = self.create_cluster(self.rc, self.read_config(True))
        cluster.start_member()
        with self.assertRaises(HazelcastError):
            await HazelcastClient.create_and_start(
                **get_ssl_config(
                    cluster.id,
                    True,
                    get_abs_path(self.current_directory, "server1-cert.pem"),
                    get_abs_path(self.current_directory, "client2-cert.pem"),
                    get_abs_path(self.current_directory, "client2-key.pem"),
                )
            )

    async def test_ma_required_client_and_server_not_authenticated(self):
        cluster = self.create_cluster(self.rc, self.read_config(True))
        cluster.start_member()
        with self.assertRaises(HazelcastError):
            await HazelcastClient.create_and_start(
                **get_ssl_config(
                    cluster.id,
                    True,
                    get_abs_path(self.current_directory, "server2-cert.pem"),
                    get_abs_path(self.current_directory, "client2-cert.pem"),
                    get_abs_path(self.current_directory, "client2-key.pem"),
                )
            )

    async def test_ma_optional_client_and_server_authenticated(self):
        cluster = self.create_cluster(self.rc, self.read_config(False))
        cluster.start_member()
        client = await HazelcastClient.create_and_start(
            **get_ssl_config(
                cluster.id,
                True,
                get_abs_path(self.current_directory, "server1-cert.pem"),
                get_abs_path(self.current_directory, "client1-cert.pem"),
                get_abs_path(self.current_directory, "client1-key.pem"),
            )
        )
        self.assertTrue(client.lifecycle_service.is_running())
        await client.shutdown()

    async def test_ma_optional_server_not_authenticated(self):
        cluster = self.create_cluster(self.rc, self.read_config(False))
        cluster.start_member()
        with self.assertRaises(HazelcastError):
            await HazelcastClient.create_and_start(
                **get_ssl_config(
                    cluster.id,
                    True,
                    get_abs_path(self.current_directory, "server2-cert.pem"),
                    get_abs_path(self.current_directory, "client1-cert.pem"),
                    get_abs_path(self.current_directory, "client1-key.pem"),
                )
            )

    async def test_ma_optional_client_not_authenticated(self):
        cluster = self.create_cluster(self.rc, self.read_config(False))
        cluster.start_member()
        with self.assertRaises(HazelcastError):
            await HazelcastClient.create_and_start(
                **get_ssl_config(
                    cluster.id,
                    True,
                    get_abs_path(self.current_directory, "server1-cert.pem"),
                    get_abs_path(self.current_directory, "client2-cert.pem"),
                    get_abs_path(self.current_directory, "client2-key.pem"),
                )
            )

    async def test_ma_optional_client_and_server_not_authenticated(self):
        cluster = self.create_cluster(self.rc, self.read_config(False))
        cluster.start_member()
        with self.assertRaises(HazelcastError):
            await HazelcastClient.create_and_start(
                **get_ssl_config(
                    cluster.id,
                    True,
                    get_abs_path(self.current_directory, "server2-cert.pem"),
                    get_abs_path(self.current_directory, "client2-cert.pem"),
                    get_abs_path(self.current_directory, "client2-key.pem"),
                )
            )

    async def test_ma_required_with_no_cert_file(self):
        cluster = self.create_cluster(self.rc, self.read_config(True))
        cluster.start_member()
        with self.assertRaises(HazelcastError):
            await HazelcastClient.create_and_start(
                **get_ssl_config(
                    cluster.id, True, get_abs_path(self.current_directory, "server1-cert.pem")
                )
            )

    async def test_ma_optional_with_no_cert_file(self):
        cluster = self.create_cluster(self.rc, self.read_config(False))
        cluster.start_member()
        client = await HazelcastClient.create_and_start(
            **get_ssl_config(
                cluster.id, True, get_abs_path(self.current_directory, "server1-cert.pem")
            )
        )
        self.assertTrue(client.lifecycle_service.is_running())
        await client.shutdown()

    def read_config(self, is_ma_required):
        file_path = self.ma_req_xml if is_ma_required else self.ma_opt_xml
        with open(file_path, "r") as f:
            xml_config = f.read()
        keystore_path = get_abs_path(self.current_directory, "server1.keystore")
        truststore_path = get_abs_path(self.current_directory, "server1.truststore")
        return xml_config % (keystore_path, truststore_path)
