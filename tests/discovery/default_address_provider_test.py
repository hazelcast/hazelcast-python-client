from unittest import TestCase
from hazelcast import six
from hazelcast.core import Address
from hazelcast.connection import DefaultAddressProvider
from hazelcast.config import ClientNetworkConfig


class DefaultAddressProviderTest(TestCase):
    def setUp(self):
        self.network_config = ClientNetworkConfig()

    def test_load_addresses(self):
        self.network_config.addresses.append("192.168.0.1:5701")
        provider = DefaultAddressProvider(self.network_config)
        addresses = provider.load_addresses()
        six.assertCountEqual(self, addresses, [Address("192.168.0.1", 5701)])

    def test_load_addresses_with_multiple_addresses(self):
        self.network_config.addresses.append("192.168.0.1:5701")
        self.network_config.addresses.append("192.168.0.1:5702")
        self.network_config.addresses.append("192.168.0.2:5701")
        provider = DefaultAddressProvider(self.network_config)
        addresses = provider.load_addresses()
        six.assertCountEqual(self, addresses, [Address("192.168.0.1", 5701),
                                               Address("192.168.0.1", 5702),
                                               Address("192.168.0.2", 5701)])

    # we deal with duplicate addresses in the util/get_possible_addresses
    def test_load_addresses_with_duplicate_addresses(self):
        self.network_config.addresses.append("192.168.0.1:5701")
        self.network_config.addresses.append("192.168.0.1:5701")
        provider = DefaultAddressProvider(self.network_config)
        addresses = provider.load_addresses()
        six.assertCountEqual(self, addresses, [Address("192.168.0.1", 5701),
                                               Address("192.168.0.1", 5701)])

    def test_load_addresses_with_empty_addresses(self):
        provider = DefaultAddressProvider(self.network_config)
        addresses = provider.load_addresses()
        six.assertCountEqual(self, addresses, [])
