from unittest import TestCase
from hazelcast import six
from hazelcast.core import Address
from hazelcast.address import DefaultAddressProvider
from hazelcast.config import ClientNetworkConfig


class DefaulAddressProviderTest(TestCase):
    def setUp(self):
        self.network_config = ClientNetworkConfig()

    def test_load_addresses(self):
        self.network_config.addresses.append("192.168.0.1:5701")
        provider = DefaultAddressProvider(self.network_config, False)
        addresses = provider.load_addresses()
        six.assertCountEqual(self, addresses, [Address("192.168.0.1", 5701)])

    def test_load_addresses_with_empty_addresses_without_other_provider(self):
        provider = DefaultAddressProvider(self.network_config, False)
        addresses = provider.load_addresses()
        six.assertCountEqual(self, addresses, [])

    def test_load_addresses_with_empty_addresses_with_other_provider(self):
        provider = DefaultAddressProvider(self.network_config, True)
        addresses = provider.load_addresses()
        six.assertCountEqual(self, addresses, [Address("127.0.0.1", 5701), Address("127.0.0.1", 5702),
                                               Address("127.0.0.1", 5703)])
