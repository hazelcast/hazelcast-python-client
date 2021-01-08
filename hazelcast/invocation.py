import logging
import time
import functools

from hazelcast.errors import (
    create_error_from_message,
    HazelcastInstanceNotActiveError,
    is_retryable_error,
    TargetDisconnectedError,
    HazelcastClientNotActiveError,
    TargetNotMemberError,
    EXCEPTION_MESSAGE_TYPE,
    IndeterminateOperationStateError,
    OperationTimeoutError,
)
from hazelcast.future import Future
from hazelcast.protocol.codec import client_local_backup_listener_codec
from hazelcast.util import AtomicInteger
from hazelcast import six

_logger = logging.getLogger(__name__)


def _no_op_response_handler(_):
    pass


class Invocation(object):
    __slots__ = (
        "request",
        "timeout",
        "partition_id",
        "uuid",
        "connection",
        "event_handler",
        "future",
        "sent_connection",
        "urgent",
        "response_handler",
        "backup_acks_received",
        "backup_acks_expected",
        "pending_response",
        "pending_response_received_time",
    )

    def __init__(
        self,
        request,
        partition_id=-1,
        uuid=None,
        connection=None,
        event_handler=None,
        urgent=False,
        timeout=None,
        response_handler=_no_op_response_handler,
    ):
        self.request = request
        self.partition_id = partition_id
        self.uuid = uuid
        self.connection = connection
        self.event_handler = event_handler
        self.urgent = urgent
        self.timeout = timeout
        self.future = Future()
        self.timeout = None
        self.sent_connection = None
        self.response_handler = response_handler
        self.backup_acks_received = 0
        self.backup_acks_expected = -1
        self.pending_response = None
        self.pending_response_received_time = -1

    def set_response(self, response):
        try:
            result = self.response_handler(response)
            self.future.set_result(result)
        except Exception as e:
            self.future.set_exception(e)

    def set_exception(self, exception, traceback=None):
        self.future.set_exception(exception, traceback)


class InvocationService(object):
    _CLEAN_RESOURCES_PERIOD = 0.1

    def __init__(self, client, config, reactor):
        smart_routing = config.smart_routing
        if smart_routing:
            self._do_invoke = self._invoke_smart
        else:
            self._do_invoke = self._invoke_non_smart

        self._client = client
        self._reactor = reactor
        self._partition_service = None
        self._connection_manager = None
        self._listener_service = None
        self._check_invocation_allowed_fn = None
        self._pending = {}
        self._next_correlation_id = AtomicInteger(1)
        self._is_redo_operation = config.redo_operation
        self._invocation_timeout = config.invocation_timeout
        self._invocation_retry_pause = config.invocation_retry_pause
        self._backup_ack_to_client_enabled = smart_routing and config.backup_ack_to_client_enabled
        self._fail_on_indeterminate_state = config.fail_on_indeterminate_operation_state
        self._backup_timeout = config.operation_backup_timeout
        self._clean_resources_timer = None
        self._shutdown = False

    def init(self, partition_service, connection_manager, listener_service):
        self._partition_service = partition_service
        self._connection_manager = connection_manager
        self._listener_service = listener_service
        self._check_invocation_allowed_fn = connection_manager.check_invocation_allowed

    def start(self):
        self._start_clean_resources_timer()
        if self._backup_ack_to_client_enabled:
            self._register_backup_listener()

    def handle_client_message(self, message):
        correlation_id = message.get_correlation_id()

        start_frame = message.start_frame
        if start_frame.has_event_flag() or start_frame.has_backup_event_flag():
            self._listener_service.handle_client_message(message, correlation_id)
            return

        invocation = self._pending.get(correlation_id, None)
        if not invocation:
            _logger.warning("Got message with unknown correlation id: %s", message)
            return

        if message.get_message_type() == EXCEPTION_MESSAGE_TYPE:
            error = create_error_from_message(message)
            return self._notify_error(invocation, error)

        self._notify(invocation, message)

    def invoke(self, invocation):
        if not invocation.timeout:
            invocation.timeout = self._invocation_timeout + time.time()

        correlation_id = self._next_correlation_id.get_and_increment()
        request = invocation.request
        request.set_correlation_id(correlation_id)
        request.set_partition_id(invocation.partition_id)
        self._do_invoke(invocation)

    def shutdown(self):
        if self._shutdown:
            return

        self._shutdown = True
        if self._clean_resources_timer:
            self._clean_resources_timer.cancel()
        for invocation in list(six.itervalues(self._pending)):
            self._notify_error(invocation, HazelcastClientNotActiveError())

    def _invoke_on_partition_owner(self, invocation, partition_id):
        owner_uuid = self._partition_service.get_partition_owner(partition_id)
        if not owner_uuid:
            _logger.debug("Partition owner is not assigned yet")
            return False
        return self._invoke_on_target(invocation, owner_uuid)

    def _invoke_on_target(self, invocation, owner_uuid):
        connection = self._connection_manager.get_connection(owner_uuid)
        if not connection:
            _logger.debug("Client is not connected to target: %s", owner_uuid)
            return False
        return self._send(invocation, connection)

    def _invoke_on_random_connection(self, invocation):
        connection = self._connection_manager.get_random_connection()
        if not connection:
            _logger.debug("No connection found to invoke")
            return False
        return self._send(invocation, connection)

    def _invoke_smart(self, invocation):
        try:
            if not invocation.urgent:
                self._check_invocation_allowed_fn()

            connection = invocation.connection
            if connection:
                invoked = self._send(invocation, connection)
                if not invoked:
                    self._notify_error(
                        invocation, IOError("Could not invoke on connection %s" % connection)
                    )
                return

            if invocation.partition_id != -1:
                invoked = self._invoke_on_partition_owner(invocation, invocation.partition_id)
            elif invocation.uuid:
                invoked = self._invoke_on_target(invocation, invocation.uuid)
            else:
                invoked = self._invoke_on_random_connection(invocation)

            if not invoked:
                invoked = self._invoke_on_random_connection(invocation)

            if not invoked:
                self._notify_error(invocation, IOError("No connection found to invoke"))
        except Exception as e:
            self._notify_error(invocation, e)

    def _invoke_non_smart(self, invocation):
        try:
            if not invocation.urgent:
                self._check_invocation_allowed_fn()

            connection = invocation.connection
            if connection:
                invoked = self._send(invocation, connection)
                if not invoked:
                    self._notify_error(
                        invocation, IOError("Could not invoke on connection %s" % connection)
                    )
                return

            if not self._invoke_on_random_connection(invocation):
                self._notify_error(invocation, IOError("No connection found to invoke"))
        except Exception as e:
            self._notify_error(invocation, e)

    def _send(self, invocation, connection):
        if self._shutdown:
            raise HazelcastClientNotActiveError()

        if self._backup_ack_to_client_enabled:
            invocation.request.set_backup_aware_flag()

        message = invocation.request
        correlation_id = message.get_correlation_id()
        self._pending[correlation_id] = invocation

        if invocation.event_handler:
            self._listener_service.add_event_handler(correlation_id, invocation.event_handler)

        if not connection.send_message(message):
            if invocation.event_handler:
                self._listener_service.remove_event_handler(correlation_id)
            return False

        invocation.sent_connection = connection
        return True

    def _notify_error(self, invocation, error):
        _logger.debug("Got exception for request %s, error: %s", invocation.request, error)

        if not self._client.lifecycle_service.is_running():
            self._complete_with_error(invocation, HazelcastClientNotActiveError())
            return

        if not self._should_retry(invocation, error):
            self._complete_with_error(invocation, error)
            return

        if invocation.timeout < time.time():
            _logger.debug("Error will not be retried because invocation timed out: %s", error)
            error = OperationTimeoutError(
                "Request timed out because an error occurred "
                "after invocation timeout: %s" % error
            )
            self._complete_with_error(invocation, error)
            return

        invoke_func = functools.partial(self._do_invoke, invocation)
        self._reactor.add_timer(self._invocation_retry_pause, invoke_func)

    def _should_retry(self, invocation, error):
        if invocation.connection and isinstance(error, (IOError, TargetDisconnectedError)):
            return False

        if invocation.uuid and isinstance(error, TargetNotMemberError):
            return False

        if isinstance(error, (IOError, HazelcastInstanceNotActiveError)) or is_retryable_error(
            error
        ):
            return True

        if isinstance(error, TargetDisconnectedError):
            return invocation.request.retryable or self._is_redo_operation

        return False

    def _complete_with_error(self, invocation, error):
        invocation.set_exception(error, None)
        correlation_id = invocation.request.get_correlation_id()
        self._pending.pop(correlation_id, None)

    def _register_backup_listener(self):
        codec = client_local_backup_listener_codec
        request = codec.encode_request()
        self._listener_service.register_listener(
            request,
            codec.decode_response,
            lambda reg_id: None,
            lambda m: codec.handle(m, self._backup_event_handler),
        ).result()

    def _backup_event_handler(self, correlation_id):
        invocation = self._pending.get(correlation_id, None)
        if not invocation:
            _logger.debug("Invocation not found for backup event, invocation id %s", correlation_id)
            return
        self._notify_backup_complete(invocation)

    def _notify(self, invocation, client_message):
        expected_backups = client_message.get_number_of_backup_acks()
        if expected_backups > invocation.backup_acks_received:
            invocation.pending_response_received_time = time.time()
            invocation.backup_acks_expected = expected_backups
            invocation.pending_response = client_message
            return

        self._complete(invocation, client_message)

    def _notify_backup_complete(self, invocation):
        invocation.backup_acks_received += 1
        if not invocation.pending_response:
            return

        if invocation.backup_acks_expected != invocation.backup_acks_received:
            return

        self._complete(invocation, invocation.pending_response)

    def _complete(self, invocation, client_message):
        invocation.set_response(client_message)
        correlation_id = invocation.request.get_correlation_id()
        self._pending.pop(correlation_id, None)

    def _start_clean_resources_timer(self):
        def run():
            if self._shutdown:
                return

            now = time.time()
            for invocation in list(self._pending.values()):
                connection = invocation.sent_connection
                if not connection:
                    continue

                if not connection.live:
                    error = TargetDisconnectedError(connection.close_reason)
                    self._notify_error(invocation, error)

                if self._backup_ack_to_client_enabled:
                    self._detect_and_handle_backup_timeout(invocation, now)

            self._clean_resources_timer = self._reactor.add_timer(self._CLEAN_RESOURCES_PERIOD, run)

        self._clean_resources_timer = self._reactor.add_timer(self._CLEAN_RESOURCES_PERIOD, run)

    def _detect_and_handle_backup_timeout(self, invocation, now):
        if not invocation.pending_response:
            return

        if invocation.backup_acks_expected == invocation.backup_acks_received:
            return

        expiration_time = invocation.pending_response_received_time + self._backup_timeout
        timeout_reached = 0 < expiration_time < now
        if not timeout_reached:
            return

        if self._fail_on_indeterminate_state:
            error = IndeterminateOperationStateError(
                "Invocation failed because the backup acks are missed"
            )
            self._complete_with_error(invocation, error)
            return

        self._complete(invocation, invocation.pending_response)
