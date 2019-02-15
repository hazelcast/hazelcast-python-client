from unittest import TestCase
from hazelcast.core import Address
from hazelcast.connection import DefaultAddressProvider
from hazelcast.discovery import HazelcastCloudAddressProvider
from hazelcast.config import ClientNetworkConfig
from hazelcast.util import get_provider_addresses, get_possible_addresses
from hazelcast import six


class MultipleProvidersTest(TestCase):
    def setUp(self):
        self.network_config = ClientNetworkConfig()
        self.cloud_address_provider = HazelcastCloudAddressProvider("", "", 0)
        self.cloud_address_provider.load_addresses = lambda: [Address("10.0.0.1", 5701)]

    def test_multiple_providers_with_empty_network_config_addresses(self):
        default_address_provider = DefaultAddressProvider(self.network_config)

        providers = [default_address_provider, self.cloud_address_provider]
        provider_addresses = get_provider_addresses(providers)
        six.assertCountEqual(self, provider_addresses, [Address("10.0.0.1", 5701)])

        addresses = get_possible_addresses(provider_addresses)
        six.assertCountEqual(self, addresses, [Address("10.0.0.1", 5701)])

    def test_multiple_providers_with_nonempty_network_config_addresses(self):
        self.network_config.addresses.append("127.0.0.1:5701")
        default_address_provider = DefaultAddressProvider(self.network_config)

        providers = [default_address_provider, self.cloud_address_provider]
        provider_addresses = get_provider_addresses(providers)
        six.assertCountEqual(self, provider_addresses, [Address("10.0.0.1", 5701), Address("127.0.0.1", 5701)])

        addresses = get_possible_addresses(provider_addresses)
        six.assertCountEqual(self, addresses, [Address("10.0.0.1", 5701), Address("127.0.0.1", 5701)])

    def test_multiple_providers_with_nonempty_network_config_addresses_without_port(self):
        self.network_config.addresses.append("127.0.0.1")
        default_address_provider = DefaultAddressProvider(self.network_config)

        providers = [default_address_provider, self.cloud_address_provider]
        provider_addresses = get_provider_addresses(providers)
        six.assertCountEqual(self, provider_addresses, [Address("10.0.0.1", 5701),
                                                        Address("127.0.0.1", 5701),
                                                        Address("127.0.0.1", 5702),
                                                        Address("127.0.0.1", 5703)])

        addresses = get_possible_addresses(provider_addresses)
        six.assertCountEqual(self, addresses, [Address("10.0.0.1", 5701),
                                               Address("127.0.0.1", 5701),
                                               Address("127.0.0.1", 5702),
                                               Address("127.0.0.1", 5703)])

    def test_multiple_providers_with_duplicate_network_config_addresses(self):
        self.network_config.addresses.append("127.0.0.1:5701")
        self.network_config.addresses.append("127.0.0.1:5701")
        default_address_provider = DefaultAddressProvider(self.network_config)

        providers = [default_address_provider, self.cloud_address_provider]
        provider_addresses = get_provider_addresses(providers)
        six.assertCountEqual(self, provider_addresses, [Address("10.0.0.1", 5701),
                                                        Address("127.0.0.1", 5701),
                                                        Address("127.0.0.1", 5701)])

        addresses = get_possible_addresses(provider_addresses)
        six.assertCountEqual(self, addresses, [Address("10.0.0.1", 5701),
                                               Address("127.0.0.1", 5701)])

    # When given empty addresses and members parameters, get_possible_addresses
    # returns _parse_address(DEFAULT_ADDRESS) which returns address list in this test.
    # When multiple providers exist, this case could only happens if both
    # of the address providers returns empty addresses with their load_addresses methods.
    # This case should never happen with cloud address provider but we are
    # doing this test to show the behavior.
    def test_multiple_providers_with_empty_load_addresses(self):
        default_address_provider = DefaultAddressProvider(self.network_config)
        self.cloud_address_provider.load_addresses = lambda: []

        providers = [default_address_provider, self.cloud_address_provider]
        provider_addresses = get_provider_addresses(providers)
        six.assertCountEqual(self, provider_addresses, [])

        addresses = get_possible_addresses(provider_addresses)
        six.assertCountEqual(self, addresses, [Address("127.0.0.1", 5701),
                                               Address("127.0.0.1", 5702),
                                               Address("127.0.0.1", 5703)])
