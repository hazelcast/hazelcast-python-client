from unittest import TestCase
from hazelcast.connection import DefaultAddressProvider
from hazelcast.discovery import HazelcastCloudAddressProvider
from hazelcast.config import ClientConfig
from hazelcast import HazelcastClient
from hazelcast.exception import IllegalStateError


class _TestClient(HazelcastClient):
    def _start(self):
        pass


class AddressProviderTest(TestCase):
    def test_default_config(self):
        client = _TestClient()
        self.assertTrue(isinstance(client._address_provider, DefaultAddressProvider))

    def test_with_nonempty_network_config_addresses(self):
        config = ClientConfig()
        config.network.addresses.append("127.0.0.1:5701")
        client = _TestClient(config)
        self.assertTrue(isinstance(client._address_provider, DefaultAddressProvider))

    def test_enabled_cloud_config(self):
        config = ClientConfig()
        config.network.cloud.enabled = True
        client = _TestClient(config)
        self.assertTrue(isinstance(client._address_provider, HazelcastCloudAddressProvider))

    def test_multiple_providers(self):
        config = ClientConfig()
        config.network.cloud.enabled = True
        config.network.addresses.append("127.0.0.1")
        with self.assertRaises(IllegalStateError):
            _TestClient(config)
