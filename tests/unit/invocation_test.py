import unittest

from mock import MagicMock

from hazelcast.config import _Config
from hazelcast.errors import IndeterminateOperationStateError
from hazelcast.invocation import Invocation, InvocationService


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
        invocation.response_handler = MagicMock(return_value=42)
        service._notify(invocation, response)
        invocation.future.set_result.assert_called_once_with(42)

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
        invocation.response_handler = MagicMock(return_value=42)
        service._notify(invocation, response)
        invocation.future.set_result.assert_called_once_with(42)

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
        invocation = MagicMock(backup_acks_received=1, backup_acks_expected=2, pending_response="x")
        invocation.response_handler = MagicMock(return_value=42)
        service._notify_backup_complete(invocation)
        invocation.future.set_result.assert_called_once_with(42)
        self.assertEqual(2, invocation.backup_acks_received)

    def test_backup_handler_when_all_acks_are_received(self):
        _, service = self._start_service()
        invocation = MagicMock(backup_acks_received=1, backup_acks_expected=1, pending_response="x")
        service._detect_and_handle_backup_timeout(invocation, 0)
        invocation.set_response.assert_not_called()

    def test_backup_handler_when_all_acks_are_not_received_and_not_reached_timeout(self):
        _, service = self._start_service()
        invocation = MagicMock(
            backup_acks_received=1,
            backup_acks_expected=2,
            pending_response="x",
            pending_response_received_time=40,
        )
        service._detect_and_handle_backup_timeout(invocation, 1)  # expiration_time = 40 + 5 > 1
        invocation.set_response.assert_not_called()

    def test_backup_handler_when_all_acks_are_not_received_and_reached_timeout(self):
        _, service = self._start_service()
        invocation = MagicMock(
            backup_acks_received=1,
            backup_acks_expected=2,
            pending_response="x",
            pending_response_received_time=40,
        )
        invocation.response_handler = MagicMock(return_value=42)
        service._detect_and_handle_backup_timeout(invocation, 46)  # expiration_time = 40 + 5 < 46
        invocation.future.set_result.assert_called_once_with(42)

    def test_backup_handler_when_all_acks_are_not_received_and_reached_timeout_with_fail_on_indeterminate_state(
        self,
    ):
        _, service = self._start_service()
        service._fail_on_indeterminate_state = True
        invocation = MagicMock(
            backup_acks_received=1,
            backup_acks_expected=2,
            pending_response="x",
            pending_response_received_time=40,
        )
        invocation.response_handler = MagicMock(return_value=42)
        service._detect_and_handle_backup_timeout(invocation, 46)  # expiration_time = 40 + 5 < 46
        invocation.future.set_result.assert_not_called()
        invocation.future.set_exception.assert_called_once()
        self.assertIsInstance(
            invocation.future.set_exception.call_args[0][0], IndeterminateOperationStateError
        )

    def test_constructor_with_timeout(self):
        invocation = Invocation(None, timeout=42)
        self.assertEqual(42, invocation.timeout)

    def _start_service(self, config=_Config()):
        c = MagicMock()
        invocation_service = InvocationService(c, config, c._reactor)
        self.service = invocation_service
        invocation_service.init(
            c._internal_partition_service,
            c._connection_manager,
            c._listener_service,
            c.compact_schema_service,
        )
        invocation_service.start()
        invocation_service.add_backup_listener()
        return c, invocation_service
