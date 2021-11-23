from unittest import TestCase

from hazelcast.core import Address
from hazelcast.discovery import HazelcastCloudDiscovery, HazelcastCloudAddressProvider


class HazelcastCloudProviderTest(TestCase):
    expected_addresses = dict()
    cloud_discovery = None
    provider = None
    private_address = Address("127.0.0.1", 5701)
    public_address = Address("192.168.0.1", 5701)

    def setUp(self):
        self.expected_addresses[Address("10.0.0.1", 5701)] = Address("198.51.100.1", 5701)
        self.expected_addresses[Address("10.0.0.1", 5702)] = Address("198.51.100.1", 5702)
        self.expected_addresses[Address("10.0.0.2", 5701)] = Address("198.51.100.2", 5701)
        self.expected_addresses[self.private_address] = self.public_address
        self.cloud_discovery = HazelcastCloudDiscovery("", 0)
        self.cloud_discovery.discover_nodes = lambda: self.expected_addresses
        self.provider = HazelcastCloudAddressProvider("", 0)
        self.provider.cloud_discovery = self.cloud_discovery

    def test_load_addresses(self):
        addresses, secondaries = self.provider.load_addresses()

        self.assertEqual(4, len(addresses))
        self.assertEqual(0, len(secondaries))
        self.assertCountEqual(list(self.expected_addresses.keys()), addresses)

    def test_load_addresses_with_exception(self):
        self.provider.cloud_discovery.discover_nodes = self.mock_discover_nodes_with_exception
        addresses, secondaries = self.provider.load_addresses()
        self.assertEqual(0, len(addresses))
        self.assertEqual(0, len(secondaries))

    def test_translate_when_address_is_none(self):
        actual = self.provider.translate(None)

        self.assertIsNone(actual)

    def test_translate(self):
        actual = self.provider.translate(self.private_address)

        self.assertEqual(self.public_address, actual)

    def test_refresh_and_translate(self):
        self.provider.refresh()
        actual = self.provider.translate(self.private_address)

        self.assertEqual(self.public_address, actual)

    def test_translate_when_not_found(self):
        not_available_address = Address("127.0.0.3", 5701)
        actual = self.provider.translate(not_available_address)

        self.assertIsNone(actual)

    def test_refresh_with_exception(self):
        cloud_discovery = HazelcastCloudDiscovery("", 0)
        cloud_discovery.discover_nodes = self.mock_discover_nodes_with_exception
        provider = HazelcastCloudAddressProvider("", 0)
        provider.cloud_discovery = cloud_discovery
        provider.refresh()

    def mock_discover_nodes_with_exception(self):
        raise Exception("Expected exception")
