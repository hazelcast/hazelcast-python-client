import time

import hazelcast
from hazelcast.config import ClientProperties
from hazelcast.errors import HazelcastTimeoutError
from hazelcast.invocation import Invocation
from hazelcast.protocol.client_message import OutboundMessage
from tests.base import HazelcastTestCase


class InvocationTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc, None)
        cls.member = cls.cluster.start_member()

    @classmethod
    def tearDownClass(cls):
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    def setUp(self):
        config = hazelcast.ClientConfig()
        config.cluster_name = self.cluster.id
        config.set_property(ClientProperties.INVOCATION_TIMEOUT_SECONDS.name, 1)
        self.client = hazelcast.HazelcastClient(config)

    def tearDown(self):
        self.client.shutdown()

    def test_invocation_timeout(self):
        request = OutboundMessage(bytearray(22), True)
        invocation_service = self.client.invocation_service
        invocation = Invocation(request, partition_id=1)

        def mock(*_):
            time.sleep(2)
            return False

        invocation_service._invoke_on_partition_owner = mock
        invocation_service._invoke_on_random_connection = lambda _: False

        invocation_service.invoke(invocation)
        with self.assertRaises(HazelcastTimeoutError):
            invocation.future.result()

    def test_invocation_not_timed_out_when_there_is_no_exception(self):
        request = OutboundMessage(bytearray(22), True)
        invocation_service = self.client.invocation_service
        invocation = Invocation(request)
        invocation_service.invoke(invocation)

        time.sleep(2)
        self.assertFalse(invocation.future.done())
        self.assertEqual(1, len(invocation_service._pending))
