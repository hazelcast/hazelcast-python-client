from unittest import TestCase
from hazelcast.core import Address
from hazelcast.discovery import HazelcastCloudDiscovery, HazelcastCloudAddressProvider


class HazelcastCloudProviderTest(TestCase):
    expected_addresses = dict()
    cloud_discovery = None
    provider = None

    def setUp(self):
        self.expected_addresses[Address("10.0.0.1", 5701)] = Address("198.51.100.1", 5701)
        self.expected_addresses[Address("10.0.0.1", 5702)] = Address("198.51.100.1", 5702)
        self.expected_addresses[Address("10.0.0.2", 5701)] = Address("198.51.100.2", 5701)
        self.cloud_discovery = HazelcastCloudDiscovery("", "", 0)
        self.cloud_discovery.discover_nodes = lambda: self.expected_addresses
        self.provider = HazelcastCloudAddressProvider("", "", 0)
        self.provider.cloud_discovery = self.cloud_discovery

    def test_load_addresses(self):
        addresses = self.provider.load_addresses()

        self.assertEqual(3, len(addresses))
        for address in self.expected_addresses.keys():
            addresses.remove(address)
        self.assertEqual(0, len(addresses))

    def test_load_addresses_with_exception(self):
        self.provider.cloud_discovery.discover_nodes = self.mock_discover_nodes_with_exception
        addresses = self.provider.load_addresses()
        self.assertEqual(0, len(addresses))

    def mock_discover_nodes_with_exception(self):
        raise Exception("Expected exception")
