from unittest import TestCase
from hazelcast.core import Address
from hazelcast.discovery import HazelcastCloudDiscovery, HazelcastCloudAddressTranslator


class HazelcastCloudTranslatorTest(TestCase):
    lookup = dict()
    private_address = None
    public_address = None
    logger = None
    translator = None

    def setUp(self):
        self.private_address = Address("127.0.0.1", 5701)
        self.public_address = Address("192.168.0.1", 5701)
        self.lookup[self.private_address] = self.public_address
        self.lookup[Address("127.0.0.2", 5701)] = Address("192.168.0.2", 5701)
        self.cloud_discovery = HazelcastCloudDiscovery("", "", 0)
        self.cloud_discovery.discover_nodes = lambda: self.lookup
        self.translator = HazelcastCloudAddressTranslator("", "", 0)
        self.translator.cloud_discovery = self.cloud_discovery

    def test_translate_when_address_is_none(self):
        actual = self.translator.translate(None)

        self.assertIsNone(actual)

    def test_translate(self):
        actual = self.translator.translate(self.private_address)

        self.assertEqual(self.public_address.host, actual.host)
        self.assertEqual(self.private_address.port, actual.port)

    def test_refresh_and_translate(self):
        self.translator.refresh()
        actual = self.translator.translate(self.private_address)

        self.assertEqual(self.public_address.host, actual.host)
        self.assertEqual(self.private_address.port, actual.port)

    def test_translate_when_not_found(self):
        not_available_address = Address("127.0.0.3", 5701)
        actual = self.translator.translate(not_available_address)

        self.assertIsNone(actual)

    def test_refresh_with_exception(self):
        cloud_discovery = HazelcastCloudDiscovery("", "", 0)
        cloud_discovery.discover_nodes = self.mock_discover_nodes_with_exception
        translator = HazelcastCloudAddressTranslator("", "", 0)
        translator.cloud_discovery = cloud_discovery
        translator.refresh()

    def mock_discover_nodes_with_exception(self):
        raise Exception("Expected exception")
