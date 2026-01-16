import os
import unittest

import pytest

from tests.integration.asyncio.base import HazelcastTestCase
from hazelcast.internal.asyncio_client import HazelcastClient
from hazelcast.errors import HazelcastError
from hazelcast.config import SSLProtocol
from tests.util import get_ssl_config, get_abs_path
from tests.integration.asyncio.util import fill_map


@pytest.mark.enterprise
class SSLTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
    current_directory = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../backward_compatible/ssl_tests")
    )
    rc = None
    hazelcast_ssl_xml = get_abs_path(current_directory, "hazelcast-ssl.xml")
    default_ca_xml = get_abs_path(current_directory, "hazelcast-default-ca.xml")

    def setUp(self):
        self.rc = self.create_rc()

    def tearDown(self):
        self.rc.exit()

    async def test_ssl_disabled(self):
        cluster = self.create_cluster(self.rc, self.read_ssl_config())
        cluster.start_member()

        with self.assertRaises(HazelcastError):
            await HazelcastClient.create_and_start(**get_ssl_config(cluster.id, False))

    async def test_ssl_enabled_is_client_live(self):
        cluster = self.create_cluster(self.rc, self.read_ssl_config())
        cluster.start_member()

        client = await HazelcastClient.create_and_start(
            **get_ssl_config(
                cluster.id, True, get_abs_path(self.current_directory, "server1-cert.pem")
            )
        )
        self.assertTrue(client.lifecycle_service.is_running())
        await client.shutdown()

    async def test_ssl_enabled_trust_default_certificates(self):
        cluster = self.create_cluster(self.rc, self.read_default_ca_config())
        cluster.start_member()

        client = await HazelcastClient.create_and_start(**get_ssl_config(cluster.id, True))
        self.assertTrue(client.lifecycle_service.is_running())
        await client.shutdown()

    async def test_ssl_enabled_dont_trust_self_signed_certificates(self):
        # Member started with self-signed certificate
        cluster = self.create_cluster(self.rc, self.read_ssl_config())
        cluster.start_member()

        with self.assertRaises(HazelcastError):
            await HazelcastClient.create_and_start(**get_ssl_config(cluster.id, True))

    async def test_ssl_enabled_map_size(self):
        cluster = self.create_cluster(self.rc, self.read_ssl_config())
        cluster.start_member()

        client = await HazelcastClient.create_and_start(
            **get_ssl_config(
                cluster.id, True, get_abs_path(self.current_directory, "server1-cert.pem")
            )
        )
        test_map = await client.get_map("test_map")
        await fill_map(test_map, 10)
        self.assertEqual(await test_map.size(), 10)
        await client.shutdown()

    async def test_ssl_enabled_with_custom_ciphers(self):
        cluster = self.create_cluster(self.rc, self.read_ssl_config())
        cluster.start_member()

        client = await HazelcastClient.create_and_start(
            **get_ssl_config(
                cluster.id,
                True,
                get_abs_path(self.current_directory, "server1-cert.pem"),
                ciphers="ECDHE-RSA-AES128-SHA256:ECDHE-RSA-AES256-GCM-SHA384",
            )
        )
        self.assertTrue(client.lifecycle_service.is_running())
        await client.shutdown()

    async def test_ssl_enabled_with_invalid_ciphers(self):
        cluster = self.create_cluster(self.rc, self.read_ssl_config())
        cluster.start_member()

        with self.assertRaises(HazelcastError):
            await HazelcastClient.create_and_start(
                **get_ssl_config(
                    cluster.id,
                    True,
                    get_abs_path(self.current_directory, "server1-cert.pem"),
                    ciphers="INVALID-CIPHER1:INVALID_CIPHER2",
                )
            )

    async def test_ssl_enabled_with_protocol_mismatch(self):
        cluster = self.create_cluster(self.rc, self.read_ssl_config())
        cluster.start_member()

        # Member configured with TLSv1
        with self.assertRaises(HazelcastError):
            await HazelcastClient.create_and_start(
                **get_ssl_config(
                    cluster.id,
                    True,
                    get_abs_path(self.current_directory, "server1-cert.pem"),
                    protocol=SSLProtocol.SSLv3,
                )
            )

    def read_default_ca_config(self):
        with open(self.default_ca_xml, "r") as f:
            return f.read()

    def read_ssl_config(self):
        with open(self.hazelcast_ssl_xml, "r") as f:
            return f.read()
