import unittest

from hazelcast.core import Address, Member
from hazelcast.util import get_possible_addresses, get_provider_addresses
from hazelcast.connection import DefaultAddressProvider
from hazelcast.config import ClientNetworkConfig
from hazelcast import six


class AddressTest(unittest.TestCase):

    def setUp(self):
        self.network_config = ClientNetworkConfig()
        self.address_provider = DefaultAddressProvider(self.network_config)

    def test_no_given_address(self):
        self.network_config.addresses = []
        provider_addresses = get_provider_addresses([self.address_provider])
        addresses = get_possible_addresses(provider_addresses)
        six.assertCountEqual(self, addresses,
                              [Address("127.0.0.1", 5701), Address("127.0.0.1", 5702), Address("127.0.0.1", 5703)])

    def test_single_given_address_with_no_port(self):
        self.network_config.addresses = ["127.0.0.1"]
        provider_addresses = get_provider_addresses([self.address_provider])
        addresses = get_possible_addresses(provider_addresses)

        six.assertCountEqual(self, addresses,
                              [Address("127.0.0.1", 5701), Address("127.0.0.1", 5702), Address("127.0.0.1", 5703)])

    def test_single_address_and_port(self):
        self.network_config.addresses = ["127.0.0.1:5701"]
        provider_addresses = get_provider_addresses([self.address_provider])
        addresses = get_possible_addresses(provider_addresses)

        six.assertCountEqual(self, addresses, [Address("127.0.0.1", 5701)])

    def test_multiple_addresses(self):
        self.network_config.addresses = ["127.0.0.1:5701", "10.0.0.1"]
        provider_addresses = get_provider_addresses([self.address_provider])
        addresses = get_possible_addresses(provider_addresses)

        six.assertCountEqual(self, addresses,
                              [Address("127.0.0.1", 5701), Address("10.0.0.1", 5701), Address("10.0.0.1", 5702),
                               Address("10.0.0.1", 5703)])

    def test_multiple_addresses_non_unique(self):
        self.network_config.addresses = ["127.0.0.1:5701", "127.0.0.1:5701"]
        provider_addresses = get_provider_addresses([self.address_provider])
        addresses = get_possible_addresses(provider_addresses)

        six.assertCountEqual(self, addresses, [Address("127.0.0.1", 5701)])

    def test_addresses_and_members(self):
        self.network_config.addresses = ["127.0.0.1:5701"]
        member_list = [Member(Address("10.0.0.1", 5703), "uuid1"), Member(Address("10.0.0.2", 5701), "uuid2")]
        provider_addresses = get_provider_addresses([self.address_provider])
        addresses = get_possible_addresses(provider_addresses, member_list)

        six.assertCountEqual(self, addresses,
                              [Address("127.0.0.1", 5701), Address("10.0.0.1", 5703), Address("10.0.0.2", 5701)])
