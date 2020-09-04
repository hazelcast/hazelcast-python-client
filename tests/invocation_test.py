import time

from hazelcast.config import ClientProperties
from hazelcast.errors import HazelcastTimeoutError
from hazelcast.invocation import Invocation
from hazelcast.protocol.client_message import OutboundMessage
from tests.base import SingleMemberTestCase


class InvocationTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config.cluster_name = cls.cluster.id
        config.set_property(ClientProperties.INVOCATION_TIMEOUT_SECONDS.name, 1)
        return config

    def test_invocation_timeout(self):
        request = OutboundMessage(bytearray(22), True)
        invocation_service = self.client.invocation_service
        invocation = Invocation(request, partition_id=1)

        def mock(*args):
            time.sleep(2)
            return False

        invocation_service._invoke_on_partition_owner = mock
        invocation_service._invoke_on_random_connection = lambda i: False

        with self.assertRaises(HazelcastTimeoutError):
            invocation_service.invoke(invocation)
            invocation.future.result()
