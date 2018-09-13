from unittest import TestCase
from hazelcast.client import HazelcastClient
from hazelcast.config import ClientConfig, ClientCloudConfig
from hazelcast.config import PROPERTY_CLOUD_DISCOVERY_TOKEN, DEFAULT_CLOUD_DISCOVERY_TOKEN
from hazelcast.discovery import HazelcastCloudDiscovery
from hazelcast.discovery import PROPERTY_CLOUD_URL_BASE
from hazelcast.exception import HazelcastError


class HazelcastCloudConfigTest(TestCase):

    def setUp(self):
        self.token = "TOKEN"
        self.config = ClientConfig()

    def test_cloud_config_defaults(self):
        cloud_config = self.config.network_config.cloud_config
        self.assertEqual(False, cloud_config.enabled)
        self.assertEqual("", cloud_config.discovery_token)

    def test_cloud_config(self):
        cloud_config = ClientCloudConfig()
        cloud_config.enabled = True
        cloud_config.discovery_token = self.token
        self.config.network_config.cloud_config = cloud_config
        self.assertEqual(True, self.config.network_config.cloud_config.enabled)
        self.assertEqual(self.token, self.config.network_config.cloud_config.discovery_token)

    def test_cloud_config_with_property(self):
        self.config._properties[PROPERTY_CLOUD_DISCOVERY_TOKEN] = self.token
        self.assertEqual(False, self.config.network_config.cloud_config.enabled)
        self.assertEqual("", self.config.network_config.cloud_config.discovery_token)
        self.assertEqual(self.token, self.config.get_property_or_default(PROPERTY_CLOUD_DISCOVERY_TOKEN,
                                                                         DEFAULT_CLOUD_DISCOVERY_TOKEN))

    def test_cloud_config_with_property_and_client_configuration(self):
        self.config.network_config.cloud_config.enabled = True
        self.config._properties[PROPERTY_CLOUD_DISCOVERY_TOKEN] = self.token
        with self.assertRaises(HazelcastError):
            client = HazelcastClient(self.config)

    def test_custom_cloud_url(self):
        self.config._properties[PROPERTY_CLOUD_DISCOVERY_TOKEN] = self.token
        self.config._properties[PROPERTY_CLOUD_URL_BASE] = "dev.hazelcast.cloud"
        host, url = HazelcastCloudDiscovery.get_host_and_url(self.config._properties, self.token)
        self.assertEqual("dev.hazelcast.cloud", host)
        self.assertEqual("/cluster/discovery?token=TOKEN", url)

    def test_custom_cloud_url_with_https(self):
        self.config._properties[PROPERTY_CLOUD_DISCOVERY_TOKEN] = self.token
        self.config._properties[PROPERTY_CLOUD_URL_BASE] = "https://dev.hazelcast.cloud"
        host, url = HazelcastCloudDiscovery.get_host_and_url(self.config._properties, self.token)
        self.assertEqual("dev.hazelcast.cloud", host)
        self.assertEqual("/cluster/discovery?token=TOKEN", url)

    def test_custom_url_with_http(self):
        self.config._properties[PROPERTY_CLOUD_DISCOVERY_TOKEN] = self.token
        self.config._properties[PROPERTY_CLOUD_URL_BASE] = "http://dev.hazelcast.cloud"
        host, url = HazelcastCloudDiscovery.get_host_and_url(self.config._properties, self.token)
        self.assertEqual("dev.hazelcast.cloud", host)
        self.assertEqual("/cluster/discovery?token=TOKEN", url)

    def test_default_cloud_url(self):
        self.config._properties[PROPERTY_CLOUD_DISCOVERY_TOKEN] = self.token
        host, url = HazelcastCloudDiscovery.get_host_and_url(self.config._properties, self.token)
        self.assertEqual("coordinator.hazelcast.cloud", host)
        self.assertEqual("/cluster/discovery?token=TOKEN", url)
