from unittest import TestCase
from hazelcast.core import Address
from hazelcast.connection import DefaultAddressProvider


class DefaultAddressProviderTest(TestCase):
    def test_load_addresses(self):
        initial_list = ["192.168.0.1:5701"]
        provider = DefaultAddressProvider(initial_list)
        primaries, secondaries = provider.load_addresses()
        self.assertCountEqual(primaries, [Address("192.168.0.1", 5701)])
        self.assertCountEqual(secondaries, [])

    def test_load_addresses_with_multiple_addresses(self):
        initial_list = ["192.168.0.1:5701", "192.168.0.1:5702", "192.168.0.2:5701"]
        provider = DefaultAddressProvider(initial_list)
        primaries, secondaries = provider.load_addresses()
        self.assertCountEqual(
            primaries,
            [
                Address("192.168.0.1", 5701),
                Address("192.168.0.1", 5702),
                Address("192.168.0.2", 5701),
            ],
        )
        self.assertCountEqual(secondaries, [])

    # we deal with duplicate addresses in the ConnectionManager#_get_possible_addresses
    def test_load_addresses_with_duplicate_addresses(self):
        initial_list = ["192.168.0.1:5701", "192.168.0.1:5701"]
        provider = DefaultAddressProvider(initial_list)
        primaries, secondaries = provider.load_addresses()
        self.assertCountEqual(
            primaries, [Address("192.168.0.1", 5701), Address("192.168.0.1", 5701)]
        )
        self.assertCountEqual(secondaries, [])

    def test_load_addresses_with_empty_addresses(self):
        initial_list = []
        provider = DefaultAddressProvider(initial_list)
        primaries, secondaries = provider.load_addresses()
        self.assertCountEqual(primaries, [Address("127.0.0.1", 5701)])
        self.assertCountEqual(secondaries, [Address("127.0.0.1", 5702), Address("127.0.0.1", 5703)])

    def test_load_addresses_without_port(self):
        initial_list = ["192.168.0.1"]
        provider = DefaultAddressProvider(initial_list)
        primaries, secondaries = provider.load_addresses()
        self.assertCountEqual(primaries, [Address("192.168.0.1", 5701)])
        self.assertCountEqual(
            secondaries, [Address("192.168.0.1", 5702), Address("192.168.0.1", 5703)]
        )

    def test_translate(self):
        provider = DefaultAddressProvider([])
        address = Address("192.168.0.1", 5701)
        actual = provider.translate(address)

        self.assertEqual(address, actual)

    def test_translate_none(self):
        provider = DefaultAddressProvider([])
        actual = provider.translate(None)

        self.assertIsNone(actual)
