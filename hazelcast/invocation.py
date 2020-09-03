import logging
import time
import functools

from hazelcast.exception import create_error_from_message, HazelcastInstanceNotActiveError, is_retryable_error, \
    HazelcastTimeoutError, TargetDisconnectedError, HazelcastClientNotActiveError, TargetNotMemberError, \
    EXCEPTION_MESSAGE_TYPE
from hazelcast.future import Future
from hazelcast.util import AtomicInteger
from hazelcast import six


def _no_op_response_handler(_):
    pass


class Invocation(object):
    __slots__ = ("request", "timeout", "partition_id", "uuid", "connection", "event_handler",
                 "future", "sent_connection", "timer", "urgent", "response_handler")

    def __init__(self, request, partition_id=-1, uuid=None, connection=None,
                 event_handler=None, urgent=False, timeout=None, response_handler=_no_op_response_handler):
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
        self.timer = None
        self.response_handler = response_handler

    def set_response(self, response):
        if self.timer:
            self.timer.cancel()
        try:
            result = self.response_handler(response)
            self.future.set_result(result)
        except Exception as e:
            self.future.set_exception(e)

    def set_exception(self, exception, traceback=None):
        if self.timer:
            self.timer.cancel()
        self.future.set_exception(exception, traceback)

    def on_timeout(self):
        self.set_exception(HazelcastTimeoutError("Request timed out."))


class InvocationService(object):
    logger = logging.getLogger("HazelcastClient.InvocationService")

    def __init__(self, client):
        config = client.config
        if config.network.smart_routing:
            self.invoke = self._invoke_smart
        else:
            self.invoke = self._invoke_non_smart

        self._pending = {}
        self._next_correlation_id = AtomicInteger(1)
        self._client = client
        self._logger_extras = {"client_name": client.name, "cluster_name": config.cluster_name}
        self._is_redo_operation = config.network.redo_operation
        self._invocation_timeout = self._init_invocation_timeout()
        self._invocation_retry_pause = self._init_invocation_retry_pause()
        self._listener_service = client.listener_service
        self._partition_service = client.partition_service
        self._connection_manager = client.connection_manager
        self._check_invocation_allowed_fn = self._connection_manager.check_invocation_allowed
        self._shutdown = False

    def handle_client_message(self, message):
        correlation_id = message.get_correlation_id()

        if message.start_frame.has_event_flag():
            self._listener_service.handle_client_message(message, correlation_id)
            return

        invocation = self._pending.pop(correlation_id, None)
        if not invocation:
            self.logger.warning("Got message with unknown correlation id: %s", message, extra=self._logger_extras)
            return

        if message.get_message_type() == EXCEPTION_MESSAGE_TYPE:
            error = create_error_from_message(message)
            return self._handle_exception(invocation, error)

        invocation.set_response(message)

    def start(self):
        self._listener_service = self._client.listener_service

    def shutdown(self):
        self._shutdown = True
        for invocation in list(six.itervalues(self._pending)):
            self._handle_exception(invocation, HazelcastClientNotActiveError())

    def _invoke_on_partition_owner(self, invocation, partition_id):
        owner_uuid = self._partition_service.get_partition_owner(partition_id)
        if not owner_uuid:
            self.logger.debug("Partition owner is not assigned yet", extra=self._logger_extras)
            return False
        return self._invoke_on_target(invocation, owner_uuid)

    def _invoke_on_target(self, invocation, owner_uuid):
        connection = self._connection_manager.get_connection(owner_uuid)
        if not connection:
            self.logger.debug("Client is not connected to target: %s" % owner_uuid, extra=self._logger_extras)
            return False
        return self._send(invocation, connection)

    def _invoke_on_random_connection(self, invocation):
        connection = self._connection_manager.get_random_connection()
        if not connection:
            self.logger.debug("No connection found to invoke", extra=self._logger_extras)
            return False
        return self._send(invocation, connection)

    def _invoke_smart(self, invocation):
        if not invocation.timeout:
            invocation.timeout = self._invocation_timeout + time.time()

        try:
            if not invocation.urgent:
                self._check_invocation_allowed_fn()

            connection = invocation.connection
            if connection:
                invoked = self._send(invocation, connection)
                if not invoked:
                    self._handle_exception(invocation, IOError("Could not invoke on connection %s" % connection))
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
                self._handle_exception(invocation, IOError("No connection found to invoke"))
        except Exception as e:
            self._handle_exception(invocation, e)

    def _invoke_non_smart(self, invocation):
        if not invocation.timeout:
            invocation.timeout = self._invocation_timeout + time.time()

        try:
            if not invocation.urgent:
                self._check_invocation_allowed_fn()

            connection = invocation.connection
            if connection:
                invoked = self._send(invocation, connection)
                if not invoked:
                    self._handle_exception(invocation, IOError("Could not invoke on connection %s" % connection))
                return

            if not self._invoke_on_random_connection(invocation):
                self._handle_exception(invocation, IOError("No connection found to invoke"))
        except Exception as e:
            self._handle_exception(invocation, e)

    def _init_invocation_retry_pause(self):
        invocation_retry_pause = self._client.properties.get_seconds_positive_or_default(
            self._client.properties.INVOCATION_RETRY_PAUSE_MILLIS)
        return invocation_retry_pause

    def _init_invocation_timeout(self):
        invocation_timeout = self._client.properties.get_seconds_positive_or_default(
            self._client.properties.INVOCATION_TIMEOUT_SECONDS)
        return invocation_timeout

    def _send(self, invocation, connection):
        if self._shutdown:
            raise HazelcastClientNotActiveError()

        correlation_id = self._next_correlation_id.get_and_increment()
        message = invocation.request
        message.set_correlation_id(correlation_id)
        message.set_partition_id(invocation.partition_id)

        self._pending[correlation_id] = invocation
        if not invocation.timer:
            invocation.timer = self._client.reactor.add_timer_absolute(invocation.timeout, invocation.on_timeout)

        if invocation.event_handler:
            self._listener_service.add_event_handler(correlation_id, invocation.event_handler)

        self.logger.debug("Sending %s to %s", message, connection, extra=self._logger_extras)

        if not connection.send_message(message):
            if invocation.event_handler:
                self._listener_service.remove_event_handler(correlation_id)
            return False
        return True

    def _handle_exception(self, invocation, error, traceback=None):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Got exception for request %s, error: %s" % (invocation.request, error),
                              extra=self._logger_extras)

        if not self._client.lifecycle_service.running:
            invocation.set_exception(HazelcastClientNotActiveError(), traceback)
            self._pending.pop(invocation.request.get_correlation_id(), None)
            return

        if not self._should_retry(invocation, error):
            invocation.set_exception(error, traceback)
            self._pending.pop(invocation.request.get_correlation_id(), None)
            return

        if invocation.timeout < time.time():
            self.logger.debug("Error will not be retried because invocation timed out: %s", error,
                              extra=self._logger_extras)
            invocation.set_exception(HazelcastTimeoutError("Request timed out because an error occurred after "
                                                           "invocation timeout: %s" % error, traceback))
            self._pending.pop(invocation.request.get_correlation_id(), None)
            return

        invoke_func = functools.partial(self.invoke, invocation)
        self._client.reactor.add_timer(self._invocation_retry_pause, invoke_func)

    def _should_retry(self, invocation, error):
        if invocation.connection and isinstance(error, (IOError, TargetDisconnectedError)):
            return True

        if invocation.uuid and isinstance(error, TargetNotMemberError):
            return False

        if isinstance(error, (IOError, HazelcastInstanceNotActiveError)) or is_retryable_error(error):
            return True

        if isinstance(error, TargetDisconnectedError):
            return invocation.request.retryable or self._is_redo_operation

        return False
