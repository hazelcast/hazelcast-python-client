import unittest

from tests.integration.asyncio.base import HazelcastTestCase, SingleMemberTestCase
from hazelcast.asyncio.client import HazelcastClient
from tests.hzrc.ttypes import Lang
from tests.util import compare_client_version, random_string

try:
    from hazelcast.config import Config
    from hazelcast.errors import InvalidConfigurationError
except ImportError:
    # For backward compatibility tests
    pass


class ClientLabelsTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):

    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc)
        cls.cluster.start_member()

    @classmethod
    def tearDownClass(cls):
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    async def asyncTearDown(self):
        await self.shutdown_all_clients()

    async def test_default_config(self):
        client = await self.create_client({"cluster_name": self.cluster.id})
        self.assertIsNone(self.get_labels_from_member(client._connection_manager.client_uuid))

    async def test_provided_labels_are_received(self):
        client = await self.create_client(
            {
                "cluster_name": self.cluster.id,
                "labels": [
                    "test-label",
                ],
            }
        )
        self.assertEqual(
            b"test-label", self.get_labels_from_member(client._connection_manager.client_uuid)
        )

    def get_labels_from_member(self, client_uuid):
        script = """
        var clients = instance_0.getClientService().getConnectedClients().toArray();
        for (i=0; i < clients.length; i++) {
            var client = clients[i];
            if ("%s".equals(client.getUuid().toString())) {
                result = client.getLabels().iterator().next();
                break;
            }
        }""" % str(
            client_uuid
        )
        return self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT).result


@unittest.skipIf(
    compare_client_version("4.2.2") < 0 or compare_client_version("5.0") == 0,
    "Tests the features added in 5.1 version of the client, "
    "which are backported into 4.2.2 and 5.0.1",
)
class ClientTcpMetricsTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    async def test_bytes_received(self):
        reactor = self.client._reactor
        bytes_received = reactor._bytes_received
        self.assertGreater(bytes_received, 0)
        m = await self.client.get_map(random_string())
        await m.get(random_string())
        self.assertGreater(reactor._bytes_received, bytes_received)

    async def test_bytes_sent(self):
        reactor = self.client._reactor
        bytes_sent = reactor._bytes_sent
        self.assertGreater(bytes_sent, 0)
        m = await self.client.get_map(random_string())
        m.set(random_string(), random_string())
        self.assertGreater(reactor._bytes_sent, bytes_sent)


@unittest.skipIf(
    compare_client_version("5.2") < 0,
    "Tests the features added in 5.2 version of the client",
)
class ClientConfigurationTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
    rc = None
    cluster = None

    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc, None)
        cls.cluster.start_member()

    def setUp(self):
        self.client = None

    async def asyncTearDown(self):
        if self.client:
            await self.client.shutdown()

    @classmethod
    def tearDownClass(cls):
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    async def test_keyword_args_configuration(self):
        self.client = await HazelcastClient.create_and_start(
            cluster_name=self.cluster.id,
        )
        self.assertTrue(self.client.lifecycle_service.is_running())

    async def test_configuration_object(self):
        config = Config()
        config.cluster_name = self.cluster.id
        self.client = await HazelcastClient.create_and_start(config)
        self.assertTrue(self.client.lifecycle_service.is_running())

    async def test_configuration_object_as_keyword_argument(self):
        config = Config()
        config.cluster_name = self.cluster.id
        self.client = await HazelcastClient.create_and_start(config=config)
        self.assertTrue(self.client.lifecycle_service.is_running())

    async def test_ambiguous_configuration(self):
        config = Config()
        with self.assertRaisesRegex(
            InvalidConfigurationError,
            "Ambiguous client configuration is found",
        ):
            self.client = await HazelcastClient.create_and_start(config, cluster_name="a-cluster")
