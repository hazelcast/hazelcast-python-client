import asyncio
import unittest
import uuid

from mock import patch

from hazelcast.asyncio import HazelcastClient
from hazelcast.core import Address, MemberInfo, MemberVersion, EndpointQualifier, ProtocolType
from hazelcast.errors import IllegalStateError, TargetDisconnectedError
from hazelcast.lifecycle import LifecycleState
from hazelcast.util import AtomicInteger
from tests.integration.asyncio.base import HazelcastTestCase, SingleMemberTestCase
from tests.util import random_string

# 198.51.100.0/24 is assigned as TEST-NET-2 and should be unreachable
# See: https://en.wikipedia.org/wiki/Reserved_IP_addresses
_UNREACHABLE_ADDRESS = Address("198.51.100.1", 5701)
_MEMBER_VERSION = MemberVersion(5, 0, 0)
_CLIENT_PUBLIC_ENDPOINT_QUALIFIER = EndpointQualifier(ProtocolType.CLIENT, "public")


class ConnectionManagerTranslateTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):

    rc = None
    cluster = None
    member = None

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
        self.client = None

    async def asyncTearDown(self):
        if self.client:
            await self.client.shutdown()

    async def test_translate_is_used(self):
        # It shouldn't be able to connect to cluster using unreachable
        # public address.
        with self.assertRaises(IllegalStateError):
            with patch.object(
                HazelcastClient,
                "_create_address_provider",
                return_value=StaticAddressProvider(True, self.member.address),
            ):
                self.client = await HazelcastClient.create_and_start(
                    cluster_name=self.cluster.id,
                    cluster_connect_timeout=1.0,
                    connection_timeout=1.0,
                )

    async def test_translate_is_not_used_when_getting_existing_connection(self):
        provider = StaticAddressProvider(False, self.member.address)
        with patch.object(
            HazelcastClient,
            "_create_address_provider",
            return_value=provider,
        ):
            self.client = await HazelcastClient.create_and_start(
                cluster_name=self.cluster.id,
            )
            # If the translate is used for this, it would return
            # the unreachable address and the connection attempt
            # would fail.
            provider.should_translate = True
            conn_manager = self.client._connection_manager
            conn = await conn_manager._get_or_connect_to_address(self.member.address)
            self.assertIsNotNone(conn)

    async def test_translate_is_used_when_member_has_public_client_address(self):
        self.client = await HazelcastClient.create_and_start(
            cluster_name=self.cluster.id,
            use_public_ip=True,
        )

        member = MemberInfo(
            _UNREACHABLE_ADDRESS,
            uuid.uuid4(),
            {},
            False,
            _MEMBER_VERSION,
            None,
            {
                _CLIENT_PUBLIC_ENDPOINT_QUALIFIER: self.member.address,
            },
        )
        conn_manager = self.client._connection_manager
        conn = await conn_manager._get_or_connect_to_member(member)
        self.assertIsNotNone(conn)

    async def test_translate_is_not_used_when_member_has_public_client_address_but_option_is_disabled(
        self,
    ):
        self.client = await HazelcastClient.create_and_start(
            cluster_name=self.cluster.id,
            connection_timeout=1.0,
            use_public_ip=False,
        )

        member = MemberInfo(
            _UNREACHABLE_ADDRESS,
            uuid.uuid4(),
            {},
            False,
            _MEMBER_VERSION,
            None,
            {
                _CLIENT_PUBLIC_ENDPOINT_QUALIFIER: self.member.address,
            },
        )
        conn_manager = self.client._connection_manager

        with self.assertRaises(TargetDisconnectedError):
            await conn_manager._get_or_connect_to_member(member)


class ConnectionManagerOnClusterRestartTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    async def test_client_state_is_sent_once_if_send_operation_is_successful(self):
        conn_manager = self.client._connection_manager
        counter = AtomicInteger()

        async def send_state_to_cluster_fn():
            counter.add(1)
            return None

        conn_manager._send_state_to_cluster_fn = send_state_to_cluster_fn
        await self._restart_cluster()
        self.assertEqual(1, counter.get())

    async def test_sending_client_state_is_retried_if_send_operation_is_failed(self):
        conn_manager = self.client._connection_manager
        counter = AtomicInteger()

        async def send_state_to_cluster_fn():
            counter.add(1)
            if counter.get() == 5:
                # Let's pretend it succeeds at some point
                return None

            raise RuntimeError("expected")

        conn_manager._send_state_to_cluster_fn = send_state_to_cluster_fn
        await self._restart_cluster()
        self.assertEqual(5, counter.get())

    async def test_sending_client_state_is_retried_if_send_operation_is_failed_synchronously(self):
        conn_manager = self.client._connection_manager
        counter = AtomicInteger()

        async def send_state_to_cluster_fn():
            counter.add(1)
            if counter.get() == 5:
                # Let's pretend it succeeds at some point
                return None

            raise RuntimeError("expected")

        conn_manager._send_state_to_cluster_fn = send_state_to_cluster_fn
        await self._restart_cluster()
        self.assertEqual(5, counter.get())

    async def test_client_state_is_sent_on_reconnection_when_the_cluster_id_is_same(self):
        disconnected = asyncio.Event()
        reconnected = asyncio.Event()

        def listener(state):
            if state == LifecycleState.DISCONNECTED:
                disconnected.set()
            elif disconnected.is_set() and state == LifecycleState.CONNECTED:
                reconnected.set()

        self.client.lifecycle_service.add_listener(listener)
        conn_manager = self.client._connection_manager
        counter = AtomicInteger()

        async def send_state_to_cluster_fn():
            counter.add(1)
            return None

        conn_manager._send_state_to_cluster_fn = send_state_to_cluster_fn

        # Keep the cluster alive, but close the connection
        # to simulate re-connection to a cluster with
        # the same cluster id.
        connection = conn_manager.get_random_connection()
        await connection.close_connection("expected", None)

        await disconnected.wait()
        await reconnected.wait()

        await self._wait_until_state_is_sent()

        self.assertEqual(1, counter.get())

    async def _restart_cluster(self):
        await asyncio.to_thread(self.rc.terminateMember, self.cluster.id, self.member.uuid)
        ConnectionManagerOnClusterRestartTest.member = await asyncio.to_thread(
            self.cluster.start_member
        )
        await self._wait_until_state_is_sent()

    async def _wait_until_state_is_sent(self):
        # Perform an invocation to wait until the client state is sent
        m = await self.client.get_map(random_string())
        await m.set(1, 1)
        self.assertEqual(1, await m.get(1))


class StaticAddressProvider:
    def __init__(self, should_translate, member_address):
        self.should_translate = should_translate
        self.member_address = member_address

    async def load_addresses(self):
        return [self.member_address], []

    async def translate(self, address):
        if not self.should_translate:
            return address

        if address == self.member_address:
            return _UNREACHABLE_ADDRESS

        return None
