import time
import unittest

from mock import MagicMock

import hazelcast
from hazelcast.config import _Config
from hazelcast.errors import IndeterminateOperationStateError, OperationTimeoutError
from hazelcast.invocation import Invocation, InvocationService
from hazelcast.protocol.client_message import OutboundMessage
from hazelcast.serialization import LE_INT
from tests.base import HazelcastTestCase


class InvocationTest(unittest.TestCase):
    def setUp(self):
        self.service = None

    def tearDown(self):
        if self.service:
            self.service.shutdown()

    def test_smart_mode_and_enabled_backups(self):
        client, service = self._start_service()
        self.assertIsNotNone(service._clean_resources_timer)
        listener_service = client._listener_service
        listener_service.register_listener.assert_called_once()

    def test_smart_mode_and_disabled_backups(self):
        config = _Config()
        config.backup_ack_to_client_enabled = False
        client, service = self._start_service(config)
        self.assertIsNotNone(service._clean_resources_timer)
        listener_service = client._listener_service
        listener_service.register_listener.assert_not_called()

    def test_unisocket_mode_and_enabled_backups(self):
        config = _Config()
        config.smart_routing = False
        client, service = self._start_service(config)
        self.assertIsNotNone(service._clean_resources_timer)
        listener_service = client._listener_service
        listener_service.register_listener.assert_not_called()

    def test_unisocket_mode_and_disabled_backups(self):
        config = _Config()
        config.smart_routing = False
        config.backup_ack_to_client_enabled = False
        client, service = self._start_service(config)
        self.assertIsNotNone(service._clean_resources_timer)
        listener_service = client._listener_service
        listener_service.register_listener.assert_not_called()

    def test_notify_with_no_expected_backups(self):
        _, service = self._start_service()
        response = MagicMock()
        response.get_number_of_backup_acks = MagicMock(return_value=0)
        invocation = MagicMock(backup_acks_received=0)
        service._notify(invocation, response)
        invocation.set_response.assert_called_once_with(response)

    def test_notify_with_expected_backups(self):
        _, service = self._start_service()
        response = MagicMock()
        response.get_number_of_backup_acks = MagicMock(return_value=1)
        invocation = MagicMock(backup_acks_received=0)
        service._notify(invocation, response)
        invocation.set_response.assert_not_called()
        self.assertTrue(invocation.pending_response_received_time > 0)
        self.assertEqual(1, invocation.backup_acks_expected)
        self.assertEqual(response, invocation.pending_response)

    def test_notify_with_equal_expected_and_received_acks(self):
        _, service = self._start_service()
        response = MagicMock()
        response.get_number_of_backup_acks = MagicMock(return_value=1)
        invocation = MagicMock(backup_acks_received=1)
        service._notify(invocation, response)
        invocation.set_response.assert_called_once_with(response)

    def test_notify_backup_complete_with_no_pending_response(self):
        _, service = self._start_service()
        invocation = MagicMock(backup_acks_received=0)
        service._notify_backup_complete(invocation)
        invocation.set_response.assert_not_called()
        self.assertEqual(1, invocation.backup_acks_received)

    def test_notify_backup_complete_with_pending_acks(self):
        _, service = self._start_service()
        invocation = MagicMock(backup_acks_received=1, backup_acks_expected=3, pending_response="x")
        service._notify_backup_complete(invocation)
        invocation.set_response.assert_not_called()
        self.assertEqual(2, invocation.backup_acks_received)

    def test_notify_backup_complete_when_all_acks_are_received(self):
        _, service = self._start_service()
        message = "x"
        invocation = MagicMock(backup_acks_received=1, backup_acks_expected=2, pending_response=message)
        service._notify_backup_complete(invocation)
        invocation.set_response.assert_called_once_with(message)
        self.assertEqual(2, invocation.backup_acks_received)

    def test_backup_handler_when_all_acks_are_received(self):
        _, service = self._start_service()
        invocation = MagicMock(backup_acks_received=1, backup_acks_expected=1, pending_response="x")
        service._detect_and_handle_backup_timeout(invocation, 0)
        invocation.set_response.assert_not_called()

    def test_backup_handler_when_all_acks_are_not_received_and_not_reached_timeout(self):
        _, service = self._start_service()
        invocation = MagicMock(backup_acks_received=1, backup_acks_expected=2, pending_response="x",
                               pending_response_received_time=40)
        service._detect_and_handle_backup_timeout(invocation, 1)  # expiration_time = 40 + 5 > 1
        invocation.set_response.assert_not_called()

    def test_backup_handler_when_all_acks_are_not_received_and_reached_timeout(self):
        _, service = self._start_service()
        message = "x"
        invocation = MagicMock(backup_acks_received=1, backup_acks_expected=2, pending_response=message,
                               pending_response_received_time=40)
        service._detect_and_handle_backup_timeout(invocation, 46)  # expiration_time = 40 + 5 < 46
        invocation.set_response.assert_called_once_with(message)

    def test_backup_handler_when_all_acks_are_not_received_and_reached_timeout_with_fail_on_indeterminate_state(self):
        _, service = self._start_service()
        service._fail_on_indeterminate_state = True
        invocation = MagicMock(backup_acks_received=1, backup_acks_expected=2, pending_response="x",
                               pending_response_received_time=40)
        service._detect_and_handle_backup_timeout(invocation, 46)  # expiration_time = 40 + 5 < 46
        invocation.set_response.assert_not_called()
        invocation.set_exception.assert_called_once()
        self.assertIsInstance(invocation.set_exception.call_args[0][0], IndeterminateOperationStateError)

    def _start_service(self, config=_Config()):
        c = MagicMock(config=config)
        invocation_service = InvocationService(c, c._reactor)
        self.service = invocation_service
        invocation_service.init(c._internal_partition_service, c._connection_manager, c._listener_service)
        invocation_service.start()
        return c, invocation_service


class InvocationTimeoutTest(HazelcastTestCase):
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
        self.client = hazelcast.HazelcastClient(cluster_name=self.cluster.id, invocation_timeout=1)

    def tearDown(self):
        self.client.shutdown()

    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        config["invocation_timeout"] = 1
        return config

    def test_invocation_timeout(self):
        request = OutboundMessage(bytearray(22), True)
        invocation_service = self.client._invocation_service
        invocation = Invocation(request, partition_id=1)

        def mock(*_):
            time.sleep(2)
            return False

        invocation_service._invoke_on_partition_owner = MagicMock(side_effect=mock)
        invocation_service._invoke_on_random_connection = MagicMock(return_value=False)

        invocation_service.invoke(invocation)
        with self.assertRaises(OperationTimeoutError):
            invocation.future.result()

    def test_invocation_not_timed_out_when_there_is_no_exception(self):
        buf = bytearray(22)
        LE_INT.pack_into(buf, 0, 22)
        request = OutboundMessage(buf, True)
        invocation_service = self.client._invocation_service
        invocation = Invocation(request)
        invocation_service.invoke(invocation)

        time.sleep(2)
        self.assertFalse(invocation.future.done())
        self.assertEqual(1, len(invocation_service._pending))
