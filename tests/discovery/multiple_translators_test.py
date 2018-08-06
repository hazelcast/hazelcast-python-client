from unittest import TestCase
from hazelcast.config import ClientConfig
from hazelcast.exception import HazelcastError
from hazelcast.client import HazelcastClient


class MultipleTranslatorsTest(TestCase):
    def test_multiple_translators(self):
        config = ClientConfig()
        config.network_config.addresses.append("127.0.0.1:5701")
        config.network_config.cloud_config.enabled = True

        with self.assertRaises(HazelcastError):
            client = HazelcastClient(config)
