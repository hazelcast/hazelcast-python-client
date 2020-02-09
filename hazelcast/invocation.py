import logging
import threading
import time
import functools

from hazelcast.exception import create_exception, HazelcastInstanceNotActiveError, is_retryable_error, TimeoutError, \
    TargetDisconnectedError, HazelcastClientNotActiveException, TargetNotMemberError
from hazelcast.future import Future
from hazelcast.lifecycle import LIFECYCLE_STATE_CONNECTED
from hazelcast.protocol.client_message import LISTENER_FLAG
from hazelcast.protocol.custom_codec import EXCEPTION_MESSAGE_TYPE, ErrorCodec
from hazelcast.util import AtomicInteger
from hazelcast.six.moves import queue
from hazelcast import six


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
        self._logger_extras = {"client_name": client.name, "group_name": client.config.group_config.name}
        self._event_queue = queue.Queue()
        self._is_redo_operation = client.config.network_config.redo_operation
        self.invocation_retry_pause = self._init_invocation_retry_pause()
        self.invocation_timeout = self._init_invocation_timeout()
        self._listener_service = None

        if client.config.network_config.smart_routing:
            self.invoke = self.invoke_smart
        else:
            self.invoke = self.invoke_non_smart

        self._client.connection_manager.add_listener(on_connection_closed=self.cleanup_connection)
        client.heartbeat.add_listener(on_heartbeat_stopped=self._heartbeat_stopped)

    def start(self):
        self._listener_service = self._client.listener

    def invoke_on_connection(self, message, connection, ignore_heartbeat=False, event_handler=None):
        return self.invoke(Invocation(self, message, connection=connection, event_handler=event_handler),
                           ignore_heartbeat)

    def invoke_on_partition(self, message, partition_id, invocation_timeout=None):
        invocation = Invocation(self, message, partition_id=partition_id)
        if invocation_timeout:
            invocation.set_timeout(invocation_timeout)
        return self.invoke(invocation)

    def invoke_on_random_target(self, message):
        return self.invoke(Invocation(self, message))

    def invoke_on_target(self, message, address):
        return self.invoke(Invocation(self, message, address=address))

    def invoke_smart(self, invocation, ignore_heartbeat=False):
        if invocation.has_connection():
            self._send(invocation, invocation.connection, ignore_heartbeat)
        elif invocation.has_partition_id():
            addr = self._client.partition_service.get_partition_owner(invocation.partition_id)
            if addr is None:
                self._handle_exception(invocation, IOError("Partition does not have an owner. "
                                                           "partition Id: ".format(invocation.partition_id)))
            elif not self._is_member(addr):
                self._handle_exception(invocation, TargetNotMemberError("Partition owner '{}' "
                                                                        "is not a member.".format(addr)))
            else:
                self._send_to_address(invocation, addr)
        elif invocation.has_address():
            if not self._is_member(invocation.address):
                self._handle_exception(invocation, TargetNotMemberError("Target '{}' is not a member.".format
                                                                        (invocation.address)))
            else:
                self._send_to_address(invocation, invocation.address)
        else:  # send to random address
            addr = self._client.load_balancer.next_address()
            if addr is None:
                self._handle_exception(invocation, IOError("No address found to invoke"))
            else:
                self._send_to_address(invocation, addr)
        return invocation.future

    def invoke_non_smart(self, invocation, ignore_heartbeat=False):
        if invocation.has_connection():
            self._send(invocation, invocation.connection, ignore_heartbeat)
        else:
            addr = self._client.cluster.owner_connection_address
            self._send_to_address(invocation, addr)
        return invocation.future

    def cleanup_connection(self, connection, cause):
        for correlation_id, invocation in six.iteritems(dict(self._pending)):
            if invocation.sent_connection == connection:
                self._handle_exception(invocation, cause)

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

    def _send_to_address(self, invocation, address, ignore_heartbeat=False):
        try:
            conn = self._client.connection_manager.connections[address]
            self._send(invocation, conn, ignore_heartbeat)
        except KeyError:
            if self._client.lifecycle.state != LIFECYCLE_STATE_CONNECTED:
                self._handle_exception(invocation, IOError("Client is not in connected state"))
            else:
                self._client.connection_manager.get_or_connect(address).continue_with(self.on_connect, invocation,
                                                                                      ignore_heartbeat)

    def on_connect(self, f, invocation, ignore_heartbeat):
        if f.is_success():
            self._send(invocation, f.result(), ignore_heartbeat)
        else:
            self._handle_exception(invocation, f.exception(), f.traceback())

    def _send(self, invocation, connection, ignore_heartbeat):
        correlation_id = self._next_correlation_id.get_and_increment()
        message = invocation.request
        message.set_correlation_id(correlation_id)
        message.set_partition_id(invocation.partition_id)
        self._pending[correlation_id] = invocation
        if not invocation.timer:
            invocation.timer = self._client.reactor.add_timer_absolute(invocation.timeout, invocation.on_timeout)

        if invocation.event_handler is not None:
            self._listener_service.add_event_handler(correlation_id, invocation.event_handler)

        self.logger.debug("Sending %s to %s", message, connection, extra=self._logger_extras)

        if not ignore_heartbeat and not connection.heartbeating:
            self._handle_exception(invocation, TargetDisconnectedError("%s has stopped heart beating." % connection))
            return

        invocation.sent_connection = connection
        try:
            connection.send_message(message)
        except IOError as e:
            if invocation.event_handler is not None:
                self._listener_service.remove_event_handler(correlation_id)
            self._handle_exception(invocation, e)

    def _handle_client_message(self, message):
        correlation_id = message.get_correlation_id()
        if message.has_flags(LISTENER_FLAG):
            self._listener_service.handle_client_message(message)
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

        if not self._client.lifecycle.is_live:
            invocation.set_exception(HazelcastClientNotActiveException(error.args[0]), traceback)
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
            invocation.set_excetion(TimeoutError(
                '%s timed out because an error occurred after invocation timeout: %s' % (invocation.request, error),
                traceback))
            return

        invoke_func = functools.partial(self.invoke, invocation)
        self._client.reactor.add_timer(self.invocation_retry_pause, invoke_func)

    def _should_retry(self, invocation, error):
        if isinstance(error, (IOError, HazelcastInstanceNotActiveError)) or is_retryable_error(error):
            return True

        if isinstance(error, TargetDisconnectedError):
            return invocation.request.is_retryable() or self._is_redo_operation

        return False

    def _is_not_allowed_to_retry_on_selection(self, invocation, error):
        if invocation.connection is not None and isinstance(error, IOError):
            return True

        # When invocation is sent over an address,error is the TargetNotMemberError and the
        # member is not in the member list, we should not retry
        return invocation.address is not None and isinstance(error, TargetNotMemberError) \
               and not self._is_member(invocation.address)

    def _is_member(self, address):
        return self._client.cluster.get_member_by_address(address) is not None
