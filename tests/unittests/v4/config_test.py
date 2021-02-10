import unittest

from hazelcast.client import HazelcastClient
from hazelcast.config import Config
from hazelcast.serialization.api import StreamSerializer


class ConfigTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = Config()

    def test_use_client_config(self):
        self.config.client_name = "test-client"
        client = HazelcastClient(self.config, auto_connect=False)
        self.assertEqual(client._config.client_name, "test-client")

    def test_set_cluster_members_failure(self):
        with self.assertRaises(TypeError) as cm:
            self.config.cluster_members = {}
        self.assertEqual("cluster_members must be a list", cm.exception.args[0])
        with self.assertRaises(TypeError) as cm:
            self.config.cluster_members = [1, 2, 3]
        self.assertEqual("cluster_members must be a list of strings", cm.exception.args[0])

    def test_set_cluster_members_success(self):
        self.config.cluster_members = ["a", "b", "c"]

    def test_set_cluster_name_failure(self):
        with self.assertRaises(TypeError) as cm:
            self.config.cluster_name = 100
        self.assertEqual("cluster_name must be a string", cm.exception.args[0])

    def test_set_cluster_name_success(self):
        self.config.cluster_name = "foo"

    def test_set_redo_operation_failure(self):
        with self.assertRaises(TypeError) as cm:
            self.config.redo_operation = "bar"
        self.assertEqual("redo_operation must be a boolean", cm.exception.args[0])

    def test_set_redo_operation_success(self):
        self.config.redo_operation = True

    def test_set_custom_serializer_failure(self):
        class SS(StreamSerializer):
            pass

        with self.assertRaises(TypeError) as cm:
            self.config.custom_serializers = {"foo": SS}
        self.assertEqual("Keys of custom_serializers must be types", cm.exception.args[0])

        with self.assertRaises(TypeError) as cm:
            self.config.custom_serializers = {str: False}
        self.assertEqual(
            "Values of custom_serializers must be subclasses of StreamSerializer",
            cm.exception.args[0],
        )

    def test_set_heartbeat_interval_failure(self):
        with self.assertRaises(ValueError) as cm:
            self.config.heartbeat_interval = 0.0
        self.assertEqual("heartbeat_interval must be positive", cm.exception.args[0])

    def test_set_heartbeat_interval_success(self):
        self.config.heartbeat_interval = 1.0

    def test_set_connection_timeout_failure(self):
        with self.assertRaises(ValueError) as cm:
            self.config.connection_timeout = -1
        self.assertEqual("connection_timeout must be non-negative", cm.exception.args[0])

    def test_set_connection_timeout_success(self):
        self.config.connection_timeout = 0.0
        self.config.connection_timeout = 1.0

    def test_set_ssl_password_failure(self):
        with self.assertRaises(TypeError) as cm:
            self.config.ssl_password = 42
        self.assertEqual(
            "ssl_password must be a string, bytes, bytearray or callable", cm.exception.args[0]
        )

    def test_set_labels_failure(self):
        with self.assertRaises(TypeError) as cm:
            self.config.labels = [1]
        self.assertEqual("labels must be a list of strs", cm.exception.args[0])

    def test_set_labels_success(self):
        self.config.labels = ["label-1", "label-2"]
