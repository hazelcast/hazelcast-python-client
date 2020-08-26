from unittest import TestCase
from hazelcast.core import Address
from hazelcast.discovery import HazelcastCloudDiscovery


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


