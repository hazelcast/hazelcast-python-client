import time
import os

from tests.base import HazelcastTestCase
from hazelcast.statistics import Statistics
from hazelcast.client import HazelcastClient
from hazelcast.config import ClientConfig, ClientProperties, NearCacheConfig
from hazelcast.core import CLIENT_VERSION, CLIENT_TYPE
from tests.hzrc.ttypes import Lang
from tests.util import random_string


class StatisticsTest(HazelcastTestCase):

    DEFAULT_STATS_PERIOD = 3
    STATS_PERIOD = 1

    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc)
        cls.member = cls.cluster.start_member()

    @classmethod
    def tearDownClass(cls):
        cls.rc.exit()

    def test_statistics_disabled_by_default(self):
        client = HazelcastClient()
        time.sleep(2 * self.DEFAULT_STATS_PERIOD)
        response = self._get_client_stats_from_server()

        self.assertTrue(response.success)
        self.assertIsNone(response.result)
        client.shutdown()

    def test_statistics_disabled_with_wrong_value(self):
        config = ClientConfig()
        config.set_property(ClientProperties.STATISTICS_ENABLED.name, "truee")
        config.set_property(ClientProperties.STATISTICS_PERIOD_SECONDS.name, self.STATS_PERIOD)
        client = HazelcastClient(config)
        time.sleep(2 * self.STATS_PERIOD)
        response = self._get_client_stats_from_server()

        self.assertTrue(response.success)
        self.assertIsNone(response.result)
        client.shutdown()

    def test_statistics_enabled(self):
        config = ClientConfig()
        config.set_property(ClientProperties.STATISTICS_ENABLED.name, True)
        client = HazelcastClient(config)
        time.sleep(2 * self.DEFAULT_STATS_PERIOD)
        response = self._get_client_stats_from_server()

        self.assertTrue(response.success)
        self.assertIsNotNone(response.result)
        client.shutdown()

    def test_statistics_enabled_with_environment_variable(self):
        environ = os.environ
        environ[ClientProperties.STATISTICS_ENABLED.name] = "true"
        environ[ClientProperties.STATISTICS_PERIOD_SECONDS.name] = str(self.STATS_PERIOD)

        client = HazelcastClient()
        time.sleep(2 * self.STATS_PERIOD)
        response = self._get_client_stats_from_server()

        self.assertTrue(response.success)
        self.assertIsNotNone(response.result)
        os.unsetenv(ClientProperties.STATISTICS_ENABLED.name)
        os.unsetenv(ClientProperties.STATISTICS_PERIOD_SECONDS.name)
        client.shutdown()

    def test_statistics_period(self):
        config = ClientConfig()
        config.set_property(ClientProperties.STATISTICS_ENABLED.name, True)
        config.set_property(ClientProperties.STATISTICS_PERIOD_SECONDS.name, self.STATS_PERIOD)
        client = HazelcastClient(config)

        time.sleep(2 * self.STATS_PERIOD)
        response1 = self._get_client_stats_from_server()
        self.assertTrue(response1.success)
        self.assertIsNotNone(response1.result)

        time.sleep(2 * self.STATS_PERIOD)
        response2 = self._get_client_stats_from_server()
        self.assertTrue(response2.success)
        self.assertIsNotNone(response2.result)

        self.assertNotEqual(response1, response2)
        client.shutdown()

    def test_statistics_enabled_with_negative_period(self):
        config = ClientConfig()
        config.set_property(ClientProperties.STATISTICS_ENABLED.name, True)
        config.set_property(ClientProperties.STATISTICS_PERIOD_SECONDS.name, -1 * self.STATS_PERIOD)
        client = HazelcastClient(config)

        time.sleep(2 * self.DEFAULT_STATS_PERIOD)
        response = self._get_client_stats_from_server()

        self.assertTrue(response.success)
        self.assertIsNotNone(response.result)
        client.shutdown()

    def test_statistics_content(self):
        config = ClientConfig()
        config.set_property(ClientProperties.STATISTICS_ENABLED.name, True)
        config.set_property(ClientProperties.STATISTICS_PERIOD_SECONDS.name, self.STATS_PERIOD)

        map_name = random_string()

        near_cache_config = NearCacheConfig(map_name)
        config.near_cache_configs[map_name] = near_cache_config

        client = HazelcastClient(config)

        test_map = client.get_map(map_name).blocking()
        time.sleep(2 * self.STATS_PERIOD)

        response = self._get_client_stats_from_server()
        self.assertTrue(response.success)
        self.assertIsNotNone(response.result)

        result = response.result.decode("utf-8")
        local_address = self._get_local_address(client)

        # Check near cache ve client statistics
        self.assertEqual(1, result.count("clientName=" + client.name))
        self.assertEqual(1, result.count("lastStatisticsCollectionTime="))
        self.assertEqual(1, result.count("enterprise=false"))
        self.assertEqual(1, result.count("clientType=" + CLIENT_TYPE))
        self.assertEqual(1, result.count("clientVersion=" + CLIENT_VERSION))
        self.assertEqual(1, result.count("clusterConnectionTimestamp="))
        self.assertEqual(1, result.count("clientAddress=" + local_address))
        self.assertEqual(1, result.count("nc." + map_name + ".creationTime"))
        self.assertEqual(1, result.count("nc." + map_name + ".evictions"))
        self.assertEqual(1, result.count("nc." + map_name + ".hits"))
        self.assertEqual(1, result.count("nc." + map_name + ".misses"))
        self.assertEqual(1, result.count("nc." + map_name + ".ownedEntryCount"))
        self.assertEqual(1, result.count("nc." + map_name + ".expirations"))
        self.assertEqual(1, result.count("nc." + map_name + ".ownedEntryMemoryCost"))

        # Check OS and runtime statistics. We cannot know what kind of statistics will be available
        # in different platforms. So, first try to get these statistics and then check the
        # response content

        s = Statistics(client)
        psutil_stats = s._get_os_and_runtime_stats()
        for stat_name in psutil_stats:
            self.assertEqual(1, result.count(stat_name))

        client.shutdown()

    def test_special_characters(self):
        config = ClientConfig()
        config.set_property(ClientProperties.STATISTICS_ENABLED.name, True)
        config.set_property(ClientProperties.STATISTICS_PERIOD_SECONDS.name, self.STATS_PERIOD)

        map_name = random_string() + ",t=es\\t"

        near_cache_config = NearCacheConfig(map_name)
        config.near_cache_configs[map_name] = near_cache_config

        client = HazelcastClient(config)
        test_map = client.get_map(map_name).blocking()

        time.sleep(2 * self.STATS_PERIOD)
        response = self._get_client_stats_from_server()

        self.assertTrue(response.success)
        self.assertIsNotNone(response.result)

        result = response.result.decode("utf-8")
        unescaped_result = self._unescape_special_chars(result)
        self.assertEqual(-1, result.find(map_name))
        self.assertNotEqual(-1, unescaped_result.find(map_name))
        client.shutdown()

    def test_near_cache_stats(self):
        config = ClientConfig()
        config.set_property(ClientProperties.STATISTICS_ENABLED.name, True)
        config.set_property(ClientProperties.STATISTICS_PERIOD_SECONDS.name, self.STATS_PERIOD)

        map_name = random_string()

        near_cache_config = NearCacheConfig(map_name)
        config.near_cache_configs[map_name] = near_cache_config

        client = HazelcastClient(config)
        test_map = client.get_map(map_name).blocking()

        time.sleep(2 * self.STATS_PERIOD)
        response = self._get_client_stats_from_server()

        self.assertTrue(response.success)
        self.assertIsNotNone(response.result)

        result = response.result.decode("utf-8")
        self.assertEqual(1, result.count("nc." + map_name + ".evictions=0"))
        self.assertEqual(1, result.count("nc." + map_name + ".hits=0"))
        self.assertEqual(1, result.count("nc." + map_name + ".misses=0"))
        self.assertEqual(1, result.count("nc." + map_name + ".ownedEntryCount=0"))
        self.assertEqual(1, result.count("nc." + map_name + ".expirations=0"))

        test_map.put(1, 2)
        test_map.get(1)  # cache miss
        test_map.get(1)  # cache hit
        test_map.get(2)  # cache miss

        time.sleep(2 * self.STATS_PERIOD)
        response = self._get_client_stats_from_server()

        self.assertTrue(response.success)
        self.assertIsNotNone(response.result)

        result = response.result.decode("utf-8")
        self.assertEqual(1, result.count("nc." + map_name + ".evictions=0"))
        self.assertEqual(1, result.count("nc." + map_name + ".hits=1"))
        self.assertEqual(1, result.count("nc." + map_name + ".misses=2"))
        self.assertEqual(1, result.count("nc." + map_name + ".ownedEntryCount=2"))
        self.assertEqual(1, result.count("nc." + map_name + ".expirations=0"))

        client.shutdown()

    def _get_client_stats_from_server(self):
        script = "client0=instance_0.getClientService().getConnectedClients()." \
                 "toArray()[0]\nresult=client0.getClientStatistics();"
        return self.rc.executeOnController(self.cluster.id, script, Lang.PYTHON)

    def _get_local_address(self, client):
        owner_address = client.cluster.owner_connection_address
        connection = client.connection_manager.get_connection(owner_address)
        host, port = connection.socket.getsockname()
        return str(host) + ":" + str(port)

    def _unescape_special_chars(self, value):
        return value.replace("\\,", ",").replace("\\=", "=").replace("\.", ".").replace("\\\\", "\\")
