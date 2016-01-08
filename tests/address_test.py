import unittest

from hazelcast.core import Address, Member
from hazelcast.util import get_possible_addresses


class AddressTest(unittest.TestCase):
    def test_no_given_address(self):
        addresses = get_possible_addresses([])

        self.assertItemsEqual(addresses,
                              [Address("127.0.0.1", 5701), Address("127.0.0.1", 5702), Address("127.0.0.1", 5703)])

    def test_single_given_address_with_no_port(self):
        addresses = ["127.0.0.1"]

        addresses = get_possible_addresses(addresses)

        self.assertItemsEqual(addresses,
                              [Address("127.0.0.1", 5701), Address("127.0.0.1", 5702), Address("127.0.0.1", 5703)])

    def test_single_address_and_port(self):
        addresses = ["127.0.0.1:5701"]

        addresses = get_possible_addresses(addresses)

        self.assertItemsEqual(addresses, [Address("127.0.0.1", 5701)])

    def test_multiple_addresses(self):
        addresses = ["127.0.0.1:5701", "10.0.0.1"]

        addresses = get_possible_addresses(addresses)

        self.assertItemsEqual(addresses,
                              [Address("127.0.0.1", 5701), Address("10.0.0.1", 5701), Address("10.0.0.1", 5702),
                               Address("10.0.0.1", 5703)])

    def test_multiple_addresses_non_unique(self):
        addresses = ["127.0.0.1:5701", "127.0.0.1:5701"]

        addresses = get_possible_addresses(addresses)

        self.assertItemsEqual(addresses, [Address("127.0.0.1", 5701)])

    def test_addresses_and_members(self):
        addresses = ["127.0.0.1:5701"]
        member_list = [Member(Address("10.0.0.1", 5703), "uuid1"), Member(Address("10.0.0.2", 5701), "uuid2")]

        addresses = get_possible_addresses(addresses, member_list)

        self.assertItemsEqual(addresses,
                              [Address("127.0.0.1", 5701), Address("10.0.0.1", 5703), Address("10.0.0.2", 5701)])
