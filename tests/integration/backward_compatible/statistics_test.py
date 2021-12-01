import itertools
import time
import zlib

from hazelcast import __version__
from hazelcast.client import HazelcastClient
from hazelcast.core import CLIENT_TYPE
from hazelcast.serialization import BE_INT, INT_SIZE_IN_BYTES
from hazelcast.statistics import Statistics
from tests.base import HazelcastTestCase
from tests.hzrc.ttypes import Lang
from tests.util import get_current_timestamp, random_string, skip_if_client_version_older_than


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
        client = HazelcastClient(cluster_name=self.cluster.id, cluster_connect_timeout=30.0)
        time.sleep(2 * self.DEFAULT_STATS_PERIOD)
        client_uuid = client._connection_manager.client_uuid

        response = self.get_client_stats_from_server(client_uuid)

        self.assertTrue(response.success)
        self.assertIsNone(response.result)
        client.shutdown()

    def test_statistics_enabled(self):
        client = HazelcastClient(
            cluster_name=self.cluster.id, cluster_connect_timeout=30.0, statistics_enabled=True
        )
        client_uuid = client._connection_manager.client_uuid

        time.sleep(2 * self.DEFAULT_STATS_PERIOD)
        self.wait_for_statistics_collection(client_uuid)

        client.shutdown()

    def test_statistics_period(self):
        client = HazelcastClient(
            cluster_name=self.cluster.id,
            cluster_connect_timeout=30.0,
            statistics_enabled=True,
            statistics_period=self.STATS_PERIOD,
        )
        client_uuid = client._connection_manager.client_uuid

        time.sleep(2 * self.STATS_PERIOD)
        response1 = self.wait_for_statistics_collection(client_uuid)

        time.sleep(2 * self.STATS_PERIOD)
        response2 = self.wait_for_statistics_collection(client_uuid)

        self.assertNotEqual(response1, response2)
        client.shutdown()

    def test_statistics_content(self):
        map_name = random_string()
        client = HazelcastClient(
            cluster_name=self.cluster.id,
            cluster_connect_timeout=30.0,
            statistics_enabled=True,
            statistics_period=self.STATS_PERIOD,
            near_caches={
                map_name: {},
            },
        )
        client_uuid = client._connection_manager.client_uuid

        client.get_map(map_name).blocking()

        time.sleep(2 * self.STATS_PERIOD)
        response = self.wait_for_statistics_collection(client_uuid)

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
        for stat_name in self.get_runtime_and_system_metrics(client):
            self.assertEqual(1, result.count(stat_name))

        client.shutdown()

    def test_special_characters(self):
        map_name = random_string() + ",t=es\\t"
        client = HazelcastClient(
            cluster_name=self.cluster.id,
            cluster_connect_timeout=30.0,
            statistics_enabled=True,
            statistics_period=self.STATS_PERIOD,
            near_caches={
                map_name: {},
            },
        )
        client_uuid = client._connection_manager.client_uuid

        client.get_map(map_name).blocking()

        time.sleep(2 * self.STATS_PERIOD)
        response = self.wait_for_statistics_collection(client_uuid)

        result = response.result.decode("utf-8")
        unescaped_result = self.unescape_special_chars(result)
        self.assertEqual(-1, result.find(map_name))
        self.assertNotEqual(-1, unescaped_result.find(map_name))
        client.shutdown()

    def test_near_cache_stats(self):
        map_name = random_string()
        client = HazelcastClient(
            cluster_name=self.cluster.id,
            cluster_connect_timeout=30.0,
            statistics_enabled=True,
            statistics_period=self.STATS_PERIOD,
            near_caches={
                map_name: {},
            },
        )
        client_uuid = client._connection_manager.client_uuid

        test_map = client.get_map(map_name).blocking()

        time.sleep(2 * self.STATS_PERIOD)
        response = self.wait_for_statistics_collection(client_uuid)

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
        response = self.wait_for_statistics_collection(client_uuid)

        result = response.result.decode("utf-8")
        self.assertEqual(1, result.count("nc." + map_name + ".evictions=0"))
        self.assertEqual(1, result.count("nc." + map_name + ".hits=1"))
        self.assertEqual(1, result.count("nc." + map_name + ".misses=2"))
        self.assertEqual(1, result.count("nc." + map_name + ".ownedEntryCount=1"))
        self.assertEqual(1, result.count("nc." + map_name + ".expirations=0"))
        self.assertEqual(1, result.count("nc." + map_name + ".invalidations=1"))
        self.assertEqual(1, result.count("nc." + map_name + ".invalidationRequests=2"))

        client.shutdown()

    def test_metrics_blob(self):
        skip_if_client_version_older_than(self, "4.2.1")

        map_name = random_string()
        client = HazelcastClient(
            cluster_name=self.cluster.id,
            cluster_connect_timeout=30.0,
            statistics_enabled=True,
            statistics_period=self.STATS_PERIOD,
            near_caches={
                map_name: {},
            },
        )
        client_uuid = client._connection_manager.client_uuid

        client.get_map(map_name).blocking()

        time.sleep(2 * self.STATS_PERIOD)
        response = self.wait_for_statistics_collection(client_uuid, get_metric_blob=True)

        result = bytearray(response.result)

        # We will try to decompress the blob according to its contract
        # to verify we have sent something that make sense

        pos = 2  # Skip the version
        dict_buf_size = BE_INT.unpack_from(result, pos)[0]
        pos += INT_SIZE_IN_BYTES

        dict_buf = result[pos : pos + dict_buf_size]
        self.assertTrue(len(dict_buf) > 0)

        pos += dict_buf_size
        pos += INT_SIZE_IN_BYTES  # Skip metric count

        metrics_buf = result[pos:]
        self.assertTrue(len(metrics_buf) > 0)

        # If we are able to decompress it, we count the blob
        # as valid.
        zlib.decompress(dict_buf)
        zlib.decompress(metrics_buf)

        client.shutdown()

    def get_metrics_blob(self, client_uuid):
        script = (
            """
        stats = instance_0.getOriginal().node.getClientEngine().getClientStatistics();
        keys = stats.keySet().toArray();
        for(i=0; i < keys.length; i++) {
            if (keys[i].toString().equals("%s")) {
                result = stats.get(keys[i]).metricsBlob();
                break;
            }
        }"""
            % client_uuid
        )

        return self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT)

    def get_client_stats_from_server(self, client_uuid):
        script = (
            """
        stats = instance_0.getOriginal().node.getClientEngine().getClientStatistics();
        keys = stats.keySet().toArray();
        for(i=0; i < keys.length; i++) {
            if (keys[i].toString().equals("%s")) {
                result = stats.get(keys[i]).clientAttributes();
                break;
            }
        }"""
            % client_uuid
        )

        return self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT)

    def unescape_special_chars(self, value):
        return (
            value.replace("\\,", ",").replace("\\=", "=").replace("\\.", ".").replace("\\\\", "\\")
        )

    def verify_response_not_empty(self, response):
        if not response.success or response.result is None:
            raise AssertionError

    def wait_for_statistics_collection(self, client_uuid, timeout=30, get_metric_blob=False):
        timeout_time = get_current_timestamp() + timeout
        while get_current_timestamp() < timeout_time:
            if get_metric_blob:
                response = self.get_metrics_blob(client_uuid)
            else:
                response = self.get_client_stats_from_server(client_uuid)

            try:
                self.verify_response_not_empty(response)
                return response
            except AssertionError:
                time.sleep(0.1)

        raise AssertionError

    def get_runtime_and_system_metrics(self, client):
        s = Statistics(client, client._config, None, None, None, None)
        try:
            # Compatibility for <4.2.1 clients
            return s._get_os_and_runtime_stats()
        except:
            return itertools.chain(s._registered_system_gauges, s._registered_process_gauges)
