import uuid

from mock import patch

from hazelcast import HazelcastClient
from hazelcast.core import Address, MemberInfo, MemberVersion, EndpointQualifier, ProtocolType
from hazelcast.errors import IllegalStateError, TargetDisconnectedError
from hazelcast.future import ImmediateFuture, ImmediateExceptionFuture
from hazelcast.util import AtomicInteger
from tests.base import HazelcastTestCase, SingleMemberTestCase
from tests.util import random_string

_UNREACHABLE_ADDRESS = Address("192.168.0.1", 5701)
_MEMBER_VERSION = MemberVersion(5, 0, 0)
_CLIENT_PUBLIC_ENDPOINT_QUALIFIER = EndpointQualifier(ProtocolType.CLIENT, "public")


class ConnectionManagerTranslateTest(HazelcastTestCase):

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

    def tearDown(self):
        if self.client:
            self.client.shutdown()

    def test_translate_is_used(self):
        # It shouldn't be able to connect to cluster using unreachable
        # public address.
        with self.assertRaises(IllegalStateError):
            with patch.object(
                HazelcastClient,
                "_create_address_provider",
                return_value=StaticAddressProvider(True, self.member.address),
            ):
                self.client = HazelcastClient(
                    cluster_name=self.cluster.id,
                    cluster_connect_timeout=1.0,
                    connection_timeout=1.0,
                )

    def test_translate_is_not_used_when_getting_existing_connection(self):
        provider = StaticAddressProvider(False, self.member.address)
        with patch.object(
            HazelcastClient,
            "_create_address_provider",
            return_value=provider,
        ):
            self.client = HazelcastClient(
                cluster_name=self.cluster.id,
            )
            # If the translate is used for this, it would return
            # the unreachable address and the connection attempt
            # would fail.
            provider.should_translate = True
            conn_manager = self.client._connection_manager
            conn = conn_manager._get_or_connect_to_address(self.member.address).result()
            self.assertIsNotNone(conn)

    def test_translate_is_used_when_member_has_public_client_address(self):
        self.client = HazelcastClient(
            cluster_name=self.cluster.id,
            use_public_ip=True,
        )

        member = MemberInfo(
            _UNREACHABLE_ADDRESS,
            uuid.uuid4(),
            [],
            False,
            _MEMBER_VERSION,
            None,
            {
                _CLIENT_PUBLIC_ENDPOINT_QUALIFIER: self.member.address,
            },
        )
        conn_manager = self.client._connection_manager
        conn = conn_manager._get_or_connect_to_member(member).result()
        self.assertIsNotNone(conn)

    def test_translate_is_not_used_when_member_has_public_client_address_but_option_is_disabled(
        self,
    ):
        self.client = HazelcastClient(
            cluster_name=self.cluster.id,
            connection_timeout=1.0,
            use_public_ip=False,
        )

        member = MemberInfo(
            _UNREACHABLE_ADDRESS,
            uuid.uuid4(),
            [],
            False,
            _MEMBER_VERSION,
            None,
            {
                _CLIENT_PUBLIC_ENDPOINT_QUALIFIER: self.member.address,
            },
        )
        conn_manager = self.client._connection_manager

        with self.assertRaises(TargetDisconnectedError):
            conn_manager._get_or_connect_to_member(member).result()


class ConnectionManagerOnClusterRestartTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    def test_client_state_is_sent_once_if_send_operation_is_successful(self):
        conn_manager = self.client._connection_manager

        counter = AtomicInteger()

        def send_state_to_cluster_fn():
            counter.add(1)
            return ImmediateFuture(None)

        conn_manager._send_state_to_cluster_fn = send_state_to_cluster_fn

        self._restart_cluster()

        self.assertEqual(1, counter.get())

    def test_sending_client_state_is_retried_if_send_operation_is_failed(self):
        conn_manager = self.client._connection_manager

        counter = AtomicInteger()

        def send_state_to_cluster_fn():
            counter.add(1)
            if counter.get() == 5:
                # Let's pretend it succeeds at some point
                return ImmediateFuture(None)

            return ImmediateExceptionFuture(RuntimeError("expected"))

        conn_manager._send_state_to_cluster_fn = send_state_to_cluster_fn

        self._restart_cluster()

        self.assertEqual(5, counter.get())

    def test_sending_client_state_is_retried_if_send_operation_is_failed_synchronously(self):
        conn_manager = self.client._connection_manager

        counter = AtomicInteger()

        def send_state_to_cluster_fn():
            counter.add(1)
            if counter.get() == 5:
                # Let's pretend it succeeds at some point
                return ImmediateFuture(None)

            raise RuntimeError("expected")

        conn_manager._send_state_to_cluster_fn = send_state_to_cluster_fn

        self._restart_cluster()

        self.assertEqual(5, counter.get())

    def _restart_cluster(self):
        self.rc.terminateMember(self.cluster.id, self.member.uuid)
        ConnectionManagerOnClusterRestartTest.member = self.cluster.start_member()

        # Perform an invocation to wait until the client state is sent
        m = self.client.get_map(random_string()).blocking()
        m.set(1, 1)
        self.assertEqual(1, m.get(1))


class StaticAddressProvider:
    def __init__(self, should_translate, member_address):
        self.should_translate = should_translate
        self.member_address = member_address

    def load_addresses(self):
        return [self.member_address], []

    def translate(self, address):
        if not self.should_translate:
            return address

        if address == self.member_address:
            return _UNREACHABLE_ADDRESS

        return None
