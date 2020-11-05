import time

from hazelcast import __version__
from hazelcast.client import HazelcastClient
from hazelcast.core import CLIENT_TYPE
from hazelcast.statistics import Statistics
from tests.base import HazelcastTestCase
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
        client = HazelcastClient(cluster_name=self.cluster.id)
        time.sleep(2 * self.DEFAULT_STATS_PERIOD)
        client_uuid = client._connection_manager.client_uuid

        response = self._get_client_stats_from_server(client_uuid)

        self.assertTrue(response.success)
        self.assertIsNone(response.result)
        client.shutdown()

    def test_statistics_enabled(self):
        client = HazelcastClient(cluster_name=self.cluster.id, statistics_enabled=True)
        client_uuid = client._connection_manager.client_uuid

        time.sleep(2 * self.DEFAULT_STATS_PERIOD)
        self._wait_for_statistics_collection(client_uuid)

        client.shutdown()

    def test_statistics_period(self):
        client = HazelcastClient(cluster_name=self.cluster.id,
                                 statistics_enabled=True,
                                 statistics_period=self.STATS_PERIOD)
        client_uuid = client._connection_manager.client_uuid

        time.sleep(2 * self.STATS_PERIOD)
        response1 = self._wait_for_statistics_collection(client_uuid)

        time.sleep(2 * self.STATS_PERIOD)
        response2 = self._wait_for_statistics_collection(client_uuid)

        self.assertNotEqual(response1, response2)
        client.shutdown()

    def test_statistics_content(self):
        map_name = random_string()
        client = HazelcastClient(cluster_name=self.cluster.id,
                                 statistics_enabled=True,
                                 statistics_period=self.STATS_PERIOD,
                                 near_caches={
                                     map_name: {},
                                 })
        client_uuid = client._connection_manager.client_uuid

        client.get_map(map_name).blocking()

        time.sleep(2 * self.STATS_PERIOD)
        response = self._wait_for_statistics_collection(client_uuid)

        result = response.result.decode("utf-8")
        info = client._internal_cluster_service.get_local_client()
        local_address = "%s:%s" % (info.address.host, info.address.port)

        # Check near cache and client statistics
        self.assertEqual(1, result.count("clientName=" + client.name))
        self.assertEqual(1, result.count("lastStatisticsCollectionTime="))
        self.assertEqual(1, result.count("enterprise=false"))
        self.assertEqual(1, result.count("clientType=" + CLIENT_TYPE))
        self.assertEqual(1, result.count("clientVersion=" + __version__))
        self.assertEqual(1, result.count("clusterConnectionTimestamp="))
        self.assertEqual(1, result.count("clientAddress=" + local_address))
        self.assertEqual(1, result.count("nc." + map_name + ".creationTime"))
        self.assertEqual(1, result.count("nc." + map_name + ".evictions"))
        self.assertEqual(1, result.count("nc." + map_name + ".hits"))
        self.assertEqual(1, result.count("nc." + map_name + ".misses"))
        self.assertEqual(1, result.count("nc." + map_name + ".ownedEntryCount"))
        self.assertEqual(1, result.count("nc." + map_name + ".expirations"))
        self.assertEqual(1, result.count("nc." + map_name + ".invalidations"))
        self.assertEqual(1, result.count("nc." + map_name + ".invalidationRequests"))
        self.assertEqual(1, result.count("nc." + map_name + ".ownedEntryMemoryCost"))

        # Check OS and runtime statistics. We cannot know what kind of statistics will be available
        # in different platforms. So, first try to get these statistics and then check the
        # response content

        s = Statistics(client, None, None, None, None)
        psutil_stats = s._get_os_and_runtime_stats()
        for stat_name in psutil_stats:
            self.assertEqual(1, result.count(stat_name))

        client.shutdown()

    def test_special_characters(self):
        map_name = random_string() + ",t=es\\t"
        client = HazelcastClient(cluster_name=self.cluster.id,
                                 statistics_enabled=True,
                                 statistics_period=self.STATS_PERIOD,
                                 near_caches={
                                     map_name: {},
                                 })
        client_uuid = client._connection_manager.client_uuid

        client.get_map(map_name).blocking()

        time.sleep(2 * self.STATS_PERIOD)
        response = self._wait_for_statistics_collection(client_uuid)

        result = response.result.decode("utf-8")
        unescaped_result = self._unescape_special_chars(result)
        self.assertEqual(-1, result.find(map_name))
        self.assertNotEqual(-1, unescaped_result.find(map_name))
        client.shutdown()

    def test_near_cache_stats(self):
        map_name = random_string()
        client = HazelcastClient(cluster_name=self.cluster.id,
                                 statistics_enabled=True,
                                 statistics_period=self.STATS_PERIOD,
                                 near_caches={
                                     map_name: {},
                                 })
        client_uuid = client._connection_manager.client_uuid

        test_map = client.get_map(map_name).blocking()

        time.sleep(2 * self.STATS_PERIOD)
        response = self._wait_for_statistics_collection(client_uuid)

        result = response.result.decode("utf-8")
        self.assertEqual(1, result.count("nc." + map_name + ".evictions=0"))
        self.assertEqual(1, result.count("nc." + map_name + ".hits=0"))
        self.assertEqual(1, result.count("nc." + map_name + ".misses=0"))
        self.assertEqual(1, result.count("nc." + map_name + ".ownedEntryCount=0"))
        self.assertEqual(1, result.count("nc." + map_name + ".expirations=0"))
        self.assertEqual(1, result.count("nc." + map_name + ".invalidations=0"))
        self.assertEqual(1, result.count("nc." + map_name + ".invalidationRequests=0"))

        test_map.put(1, 2)  # invalidation request
        test_map.get(1)  # cache miss
        test_map.get(1)  # cache hit
        test_map.put(1, 3)  # invalidation + invalidation request
        test_map.get(1)  # cache miss

        time.sleep(2 * self.STATS_PERIOD)
        response = self._wait_for_statistics_collection(client_uuid)

        result = response.result.decode("utf-8")
        self.assertEqual(1, result.count("nc." + map_name + ".evictions=0"))
        self.assertEqual(1, result.count("nc." + map_name + ".hits=1"))
        self.assertEqual(1, result.count("nc." + map_name + ".misses=2"))
        self.assertEqual(1, result.count("nc." + map_name + ".ownedEntryCount=1"))
        self.assertEqual(1, result.count("nc." + map_name + ".expirations=0"))
        self.assertEqual(1, result.count("nc." + map_name + ".invalidations=1"))
        self.assertEqual(1, result.count("nc." + map_name + ".invalidationRequests=2"))

        client.shutdown()

    def _get_client_stats_from_server(self, client_uuid):
        script = "stats = instance_0.getOriginal().node.getClientEngine().getClientStatistics()\n" \
                 "keys = stats.keySet().toArray()\n" \
                 "for(i=0; i < keys.length; i++) {\n" \
                 "  if (keys[i].toString().equals(\"%s\")) {\n" \
                 "    result = stats.get(keys[i]).clientAttributes()\n" \
                 "    break\n" \
                 "  }\n}\n" % client_uuid

        return self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT)

    def _unescape_special_chars(self, value):
        return value.replace("\\,", ",").replace("\\=", "=").replace("\\.", ".").replace("\\\\", "\\")

    def _verify_response_not_empty(self, response):
        if not response.success or response.result is None:
            raise AssertionError

    def _wait_for_statistics_collection(self, client_uuid, timeout=30):
        timeout_time = time.time() + timeout
        response = self._get_client_stats_from_server(client_uuid)
        while time.time() < timeout_time:
            try:
                self._verify_response_not_empty(response)
                return response
            except AssertionError:
                time.sleep(0.1)
                response = self._get_client_stats_from_server(client_uuid)
        raise AssertionError
