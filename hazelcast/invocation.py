import logging
import threading
import time
import functools

from hazelcast.exception import create_exception, HazelcastInstanceNotActiveError, is_retryable_error, TimeoutError, \
    TargetDisconnectedError, HazelcastClientNotActiveException, TargetNotMemberError
from hazelcast.future import Future
from hazelcast.lifecycle import LIFECYCLE_STATE_CLIENT_CONNECTED
from hazelcast.protocol.error_factory import EXCEPTION_MESSAGE_TYPE, ErrorCodec
from hazelcast.util import AtomicInteger
from hazelcast.six.moves import queue
from hazelcast import six
from hazelcast.protocol.client_message import ClientMessage, IS_EVENT_FLAG, BACKUP_AWARE_FLAG


class Invocation(object):
    sent_connection = None
    timer = None

    def __init__(self, invocation_service, request, partition_id=-1, address=None, connection=None, event_handler=None):
        self._event = threading.Event()
        self._invocation_timeout = invocation_service.invocation_timeout
        self.timeout = self._invocation_timeout + time.time()
        self.address = address
        self.connection = connection
        self.partition_id = partition_id
        self.request = request
        self.future = Future()
        self.event_handler = event_handler
        self.invoke_count = 0
        self.urgent = False

    def has_connection(self):
        return self.connection is not None

    def has_partition_id(self):
        return self.partition_id >= 0

    def has_address(self):
        return self.address is not None

    def set_response(self, response):
        if self.timer:
            self.timer.cancel()
        self.future.set_result(response)

    def set_exception(self, exception, traceback=None):
        if self.timer:
            self.timer.cancel()
        self.future.set_exception(exception, traceback)

    def set_timeout(self, timeout):
        self._invocation_timeout = timeout
        self.timeout = self._invocation_timeout + time.time()

    def on_timeout(self):
        self.set_exception(TimeoutError("Request timed out after %d seconds." % self._invocation_timeout))


class InvocationService(object):
    logger = logging.getLogger("HazelcastClient.InvocationService")

    def __init__(self, client):
        self._pending = {}
        self._next_correlation_id = AtomicInteger(1)
        self._client = client
        self._logger_extras = {"client_name": client.name, "cluster_name": client.config.cluster_name}
        self._event_queue = queue.Queue()
        self._is_redo_operation = client.config.network_config.redo_operation
        self.invocation_retry_pause = self._init_invocation_retry_pause()
        self.invocation_timeout = self._init_invocation_timeout()
        self._listener_service = None
        self.is_shutdown = False
        self.event_handlers = {}

        if client.config.network_config.smart_routing:
            self.invoke = self.invoke_smart
            self._is_backup_ack_to_client_enabled = self._client.config.is_backup_ack_to_client_enabled
        else:
            self.invoke = self.invoke_non_smart
            self._is_backup_ack_to_client_enabled = False

    def start(self):
        self._listener_service = self._client.listener

    def invoke_on_connection(self, message, connection, event_handler=None):
        return self.invoke(Invocation(self, message, connection=connection, event_handler=event_handler))

    def invoke_on_partition(self, message, partition_id):
        return self.invoke(Invocation(self, message, partition_id=partition_id))

    def invoke_on_random_target(self, message):
        return self.invoke(Invocation(self, message))

    def invoke_on_target(self, message, remote_uuid):
        assert remote_uuid
        return self.invoke(Invocation(self, message, address=remote_uuid))

    def invoke_on_partition_owner(self, invocation, partition_id):
        partition_owner = self._client.partition_service.get_partition_owner(partition_id)

        if partition_owner is None:
            self.logger.debug("Partition owner is not assigned yet", extra=self._logger_extras)
            return False

        return self._invoke_on_uuid(invocation, partition_owner)

    def get_connection(self, target=None):
        if target:
            connection = self._client.connection_manager.get_connection(target)
            if connection is None:
                raise IOError("No available connection to member " + target)
        else:
            connection = self._client.connection_manager.get_random_connection()
            if connection is None:
                raise IOError("NonSmartClientInvocationService: No connection is available.")
        return connection

    def invoke_smart(self, invocation):
        invocation.invoke_count += 1
        if invocation.has_connection():
            invoked = self._send(invocation, invocation.connection)
        elif invocation.has_partition_id():
            invoked = self.invoke_on_partition_owner(invocation, invocation.partition_id)
        elif invocation.has_address():
            invoked = self._invoke_on_uuid(invocation, invocation.address)
        else:
            invoked = self._invoke_on_random_connection(invocation)

        if not invoked:
            if not self._invoke_on_random_connection(invocation):
                self._handle_exception(invocation, IOError("No connection found to invoke"))

        return invocation.future

    def invoke_non_smart(self, invocation):
        invocation.invoke_count += 1
        if invocation.has_connection():
            self._send(invocation, invocation.connection)
        else:
            self._invoke_on_random_connection(invocation)
        return invocation.future

    def cleanup_connection(self, connection, cause):
        for correlation_id, invocation in six.iteritems(dict(self._pending)):
            if invocation.sent_connection == connection:
                self._handle_exception(invocation, cause)

    def _invoke_on_random_connection(self, invocation):
        connection = self._client.connection_manager.get_random_connection()
        if connection is None:
            self.logger.debug("No connection found to invoke")
            return False

        return self._send(invocation, connection)

    def _invoke_on_uuid(self, invocation, remote_uuid):
        connection = self._client.connection_manager.get_connection(remote_uuid)
        if connection is None:
            self.logger.debug('Client is not connected to target: {}'.format(remote_uuid), extra=self._logger_extras)
            return False

        return self._send(invocation, connection)

    def _init_invocation_retry_pause(self):
        invocation_retry_pause = self._client.properties.get_seconds_positive_or_default(
            self._client.properties.INVOCATION_RETRY_PAUSE_MILLIS)
        return invocation_retry_pause

    def _init_invocation_timeout(self):
        invocation_timeout = self._client.properties.get_seconds_positive_or_default(
            self._client.properties.INVOCATION_TIMEOUT_SECONDS)
        return invocation_timeout

    def _heartbeat_stopped(self, connection):
        for correlation_id, invocation in six.iteritems(dict(self._pending)):
            if invocation.sent_connection == connection:
                self._handle_exception(invocation,
                                       TargetDisconnectedError("%s has stopped heart beating." % connection))

    def _send_to_address(self, invocation, address):
        try:
            conn = self._client.connection_manager.connections[address]
            self._send(invocation, conn)
        except KeyError:
            if self._client.lifecycle.state != LIFECYCLE_STATE_CLIENT_CONNECTED:
                self._handle_exception(invocation, IOError("Client is not in connected state"))
            else:
                self._client.connection_manager.get_or_connect(address).continue_with(self.on_connect, invocation)

    def on_connect(self, f, invocation):
        if f.is_success():
            self._send(invocation, f.result())
        else:
            self._handle_exception(invocation, f.exception(), f.traceback())

    def _send(self, invocation, connection):
        if self.is_shutdown:
            raise HazelcastClientNotActiveException()

        if self._is_backup_ack_to_client_enabled:
            invocation.request.start_frame.flags |= BACKUP_AWARE_FLAG

        # self._register_invocation(invocation, connection)
        correlation_id = self._next_correlation_id.get_and_increment()
        message = invocation.request
        message.set_correlation_id(correlation_id)
        message.set_partition_id(invocation.partition_id)
        self._pending[correlation_id] = invocation
        if not invocation.timer:
            invocation.timer = self._client.reactor.add_timer_absolute(invocation.timeout, invocation.on_timeout)

        if invocation.event_handler is not None:
            self._register_invocation(invocation)

        self.logger.debug("Sending %s to %s", message, connection, extra=self._logger_extras)

        invocation.sent_connection = connection
        try:
            connection.send_message(message)
        except IOError as e:
            if invocation.event_handler is not None:
                self._remove_event_handler(correlation_id)
            self._handle_exception(invocation, e)
            return False

        return True

    def _register_invocation(self, invocation):
        message = invocation.request
        correlation_id = message.get_correlation_id()
        if invocation.has_partition_id():
            message.set_partition_id(invocation.partition_id)
        else:
            message.set_partition_id(-1)

        if invocation.event_handler:
            self.event_handlers[correlation_id] = invocation

        self._pending[correlation_id] = invocation

    def _remove_event_handler(self, id):
        self.event_handlers.pop(id)

    def handle_client_message(self, message):
        correlation_id = message.get_correlation_id()

        if ClientMessage.is_flag_set(message.header_flags, IS_EVENT_FLAG):
            invocation = self.event_handlers.get(correlation_id)
            if invocation:
                invocation.event_handler(message)
            return
        if correlation_id not in self._pending:
            self.logger.warning("Got message with unknown correlation id: %s", message, extra=self._logger_extras)
            return
        invocation = self._pending.pop(correlation_id)
        if message.get_message_type() == EXCEPTION_MESSAGE_TYPE:
            error = create_exception(ErrorCodec(message))
            return self._handle_exception(invocation, error)

        invocation.set_response(message)

    def _handle_event(self, invocation, message):
        try:
            invocation.event_handler(message)
        except:
            self.logger.warning("Error handling event %s", message, exc_info=True, extra=self._logger_extras)

    def _handle_exception(self, invocation, error, traceback=None):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Got exception for request %s: %s: %s", invocation.request, type(error).__name__, error,
                              extra=self._logger_extras)

        if is_retryable_error(error):
            if invocation.request.retryable or self._is_redo_operation:
                if self._try_retry(invocation):
                    return

        if self._is_not_allowed_to_retry_on_selection(invocation, error):
            invocation.set_exception(error, traceback)
            return

        if not self._should_retry(invocation, error):
            invocation.set_exception(error, traceback)
            return

        if invocation.timeout < time.time():
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug('Error will not be retried because invocation timed out: %s', error,
                                  extra=self._logger_extras)
            invocation.set_exception(TimeoutError(
                '%s timed out because an error occurred after invocation timeout: %s' % (invocation.request, error),
                traceback))
            return

        invoke_func = functools.partial(self.invoke, invocation)
        self._client.reactor.add_timer(self.invocation_retry_pause, invoke_func)

    def _should_retry(self, invocation, error):
        if isinstance(error, (IOError, HazelcastInstanceNotActiveError)) or is_retryable_error(error):
            return True

        if isinstance(error, TargetDisconnectedError):
            return invocation.request.retryable or self._is_redo_operation

        return False

    def _is_not_allowed_to_retry_on_selection(self, invocation, error):
        if invocation.connection is not None and isinstance(error, IOError):
            return True

        # When invocation is sent over an address,error is the TargetNotMemberError and the
        # member is not in the member list, we should not retry
        return invocation.address is not None and isinstance(error, TargetNotMemberError) \
               and not self._is_member(invocation.address)

    def _is_member(self, address):
        return self._client.cluster.get_member_by_uuid(address) is not None
