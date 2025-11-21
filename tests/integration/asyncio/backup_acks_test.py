import unittest

from mock import MagicMock

from hazelcast.asyncio.client import HazelcastClient
from hazelcast.errors import IndeterminateOperationStateError
from tests.base import HazelcastTestCase


class BackupAcksTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.rc.createCluster(None, None)
        cls.rc.startMember(cls.cluster.id)
        cls.rc.startMember(cls.cluster.id)

    @classmethod
    def tearDownClass(cls):
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    def setUp(self):
        self.client = None

    async def asyncTearDown(self):
        if self.client:
            await self.client.shutdown()

    async def test_smart_mode(self):
        self.client = await HazelcastClient.create_and_start(
            cluster_name=self.cluster.id,
            fail_on_indeterminate_operation_state=True,
        )
        m = await self.client.get_map("test")
        # TODO: Remove the next line once
        # https://github.com/hazelcast/hazelcast/issues/9398 is fixed
        await m.get(1)
        # it's enough for this operation to succeed
        await m.set(1, 2)

    async def test_lost_backups_on_smart_mode_with_fail_on_indeterminate_operation_state(self):
        self.client = await HazelcastClient.create_and_start(
            cluster_name=self.cluster.id,
            operation_backup_timeout=0.3,
            fail_on_indeterminate_operation_state=True,
        )
        client = self.client
        # replace backup ack handler with a mock to emulate backup acks loss
        client._invocation_service._backup_event_handler = MagicMock()
        m = await client.get_map("test")
        with self.assertRaises(IndeterminateOperationStateError):
            await m.set(1, 2)

    async def test_lost_backups_on_smart_mode_without_fail_on_indeterminate_operation_state(self):
        self.client = await HazelcastClient.create_and_start(
            cluster_name=self.cluster.id,
            operation_backup_timeout=0.3,
            fail_on_indeterminate_operation_state=False,
        )
        client = self.client
        # replace backup ack handler with a mock to emulate backup acks loss
        client._invocation_service._backup_event_handler = MagicMock()
        m = await client.get_map("test")
        # it's enough for this operation to succeed
        await m.set(1, 2)

    async def test_backup_acks_disabled(self):
        self.client = await HazelcastClient.create_and_start(
            cluster_name=self.cluster.id,
            backup_ack_to_client_enabled=False,
        )
        m = await self.client.get_map("test")
        # it's enough for this operation to succeed
        await m.set(1, 2)

    async def test_unisocket_mode(self):
        self.client = await HazelcastClient.create_and_start(
            cluster_name=self.cluster.id,
            smart_routing=False,
        )
        m = await self.client.get_map("test")
        # it's enough for this operation to succeed
        await m.set(1, 2)

    async def test_unisocket_mode_with_disabled_backup_acks(self):
        self.client = await HazelcastClient.create_and_start(
            cluster_name=self.cluster.id,
            smart_routing=False,
            backup_ack_to_client_enabled=False,
        )
        m = await self.client.get_map("test")
        # it's enough for this operation to succeed
        await m.set(1, 2)
