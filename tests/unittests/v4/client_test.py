import unittest

from hazelcast import HazelcastClient
from hazelcast.config import Config
from hazelcast.errors import IllegalArgumentError


class ClientTestCase(unittest.TestCase):
    def test_client_config(self):
        config = Config()
        config.client_name = "test-client"
        client = HazelcastClient(config, auto_connect=False)
        self.assertEqual(client._config.client_name, "test-client")

    def test_passing_both_config_and_kwargs_raises(self):
        self.assertRaises(
            IllegalArgumentError, lambda: HazelcastClient(Config(), client_name="foo")
        )

    def test_label_client(self):
        config = {"cluster_name": "cluster_id", "labels": ["test-label"]}
        client = HazelcastClient(auto_connect=False, **config)
