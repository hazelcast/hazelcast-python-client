import time

from hazelcast.config import ClientProperties
from hazelcast.exception import TimeoutError
from hazelcast.invocation import Invocation
from tests.base import SingleMemberTestCase


class InvocationTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config.set_property(ClientProperties.INVOCATION_TIMEOUT_SECONDS.name, 1)
        return config

    def test_invocation_timeout(self):
        invocation_service = self.client.invoker
        invocation = Invocation(invocation_service, None, partition_id=-1)

        def mocked_has_partition_id():
            time.sleep(2)
            return True

        invocation.has_partition_id = mocked_has_partition_id
        with self.assertRaises(TimeoutError):
            invocation_service.invoke(invocation).result()
