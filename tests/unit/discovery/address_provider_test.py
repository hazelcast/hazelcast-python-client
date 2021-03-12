from unittest import TestCase
from hazelcast.connection import DefaultAddressProvider
from hazelcast.discovery import HazelcastCloudAddressProvider
from hazelcast import HazelcastClient
from hazelcast.errors import IllegalStateError


class _TestClient(HazelcastClient):
    def _start(self):
        pass


class AddressProviderTest(TestCase):
    def test_default_config(self):
        client = _TestClient()
        self.assertTrue(isinstance(client._address_provider, DefaultAddressProvider))

    def test_with_nonempty_network_config_addresses(self):
        client = _TestClient(cluster_members=["127.0.0.1:5701"])
        self.assertTrue(isinstance(client._address_provider, DefaultAddressProvider))

    def test_enabled_cloud_config(self):
        client = _TestClient(cloud_discovery_token="TOKEN")
        self.assertTrue(isinstance(client._address_provider, HazelcastCloudAddressProvider))

    def test_multiple_providers(self):
        with self.assertRaises(IllegalStateError):
            _TestClient(cluster_members=["127.0.0.1"], cloud_discovery_token="TOKEN")
