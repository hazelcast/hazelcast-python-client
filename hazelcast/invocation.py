import logging
import threading
import time
from Queue import Queue
import functools
from hazelcast.exception import create_exception, HazelcastInstanceNotActiveError, is_retryable_error, TimeoutError, \
    AuthenticationError, TargetDisconnectedError
from hazelcast.future import Future
from hazelcast.lifecycle import LIFECYCLE_STATE_CONNECTED
from hazelcast.protocol.client_message import LISTENER_FLAG
from hazelcast.protocol.custom_codec import EXCEPTION_MESSAGE_TYPE, ErrorCodec
from hazelcast.util import AtomicInteger

INVOCATION_TIMEOUT = 120
RETRY_WAIT_TIME_IN_SECONDS = 1


class Invocation(object):
    sent_connection = None
    timer = None

    def __init__(self, request, partition_id=-1, address=None, connection=None, timeout=INVOCATION_TIMEOUT):
        self._event = threading.Event()
        self.timeout = timeout + time.time()
        self.address = address
        self.connection = connection
        self.partition_id = partition_id
        self.request = request
        self.future = Future()

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

    def on_timeout(self):
        self.set_exception(TimeoutError("Request timed out after %d seconds." % INVOCATION_TIMEOUT))


class ListenerInvocation(Invocation):
    # used for storing the original registration id across re-registrations
    registration_id = None

    def __init__(self, request, event_handler, response_decoder=None, **kwargs):
        Invocation.__init__(self, request, **kwargs)
        self.event_handler = event_handler
        self.response_decoder = response_decoder


class ListenerService(object):
    logger = logging.getLogger("ListenerService")

    def __init__(self, client):
        self._client = client
        self.registrations = {}
        pass

    def start_listening(self, request, event_handler, decode_add_listener, key=None):
        if key:
            partition_id = self._client.partition_service.get_partition_id(key)
            invocation = ListenerInvocation(request, event_handler, decode_add_listener, partition_id=partition_id)
        else:
            invocation = ListenerInvocation(request, event_handler, decode_add_listener)

        future = self._client.invoker.invoke(invocation)
        registration_id = decode_add_listener(future.result())
        invocation.registration_id = registration_id  # store the original registration id for future reference
        self.registrations[registration_id] = (registration_id, request.get_correlation_id())
        return registration_id

    def stop_listening(self, registration_id, encode_remove_listener):
        try:
            actual_id, correlation_id = self.registrations.pop(registration_id)
            self._client.invoker._remove_event_handler(correlation_id)
            # TODO: should be invoked on same node as registration?
            self._client.invoker.invoke_on_random_target(encode_remove_listener(actual_id)).result()
            return True
        except KeyError:
            return False

    def re_register_listener(self, invocation):
        registration_id = invocation.registration_id
        new_invocation = ListenerInvocation(invocation.request, invocation.event_handler, invocation.response_decoder,
                                            partition_id=invocation.partition_id)
        new_invocation.registration_id = registration_id

        # re-send the request
        def callback(f):
            if f.is_success():
                new_id = new_invocation.response_decoder(f.result())
                self.logger.debug("Re-registered listener with id %s and new_id %s for request %s",
                                  registration_id, new_id, new_invocation.request)
                self.registrations[registration_id] = (new_id, new_invocation.request.get_correlation_id())
            else:
                logging.warn("Re-registration for listener with id %s failed.", registration_id, exc_info=True)

        self.logger.debug("Re-registering listener %s for request %s", registration_id, new_invocation.request)
        self._client.invoker.invoke(new_invocation).add_done_callback(callback)


class InvocationService(object):
    logger = logging.getLogger("InvocationService")

    def __init__(self, client):
        self._pending = {}
        self._event_handlers = {}
        self._next_correlation_id = AtomicInteger(1)
        self._client = client
        self._event_queue = Queue()
        self._is_redo_operation = client.config.network_config.redo_operation

        if client.config.network_config.smart_routing:
            self.invoke = self.invoke_smart
        else:
            self.invoke = self.invoke_non_smart

        self._client.connection_manager.add_listener(on_connection_closed=self.cleanup_connection)
        client.heartbeat.add_listener(on_heartbeat_stopped=self._heartbeat_stopped)

    def invoke_on_connection(self, message, connection, ignore_heartbeat=False):
        return self.invoke(Invocation(message, connection=connection), ignore_heartbeat)

    def invoke_on_partition(self, message, partition_id):
        return self.invoke(Invocation(message, partition_id=partition_id))

    def invoke_on_random_target(self, message):
        return self.invoke(Invocation(message))

    def invoke_on_target(self, message, address):
        return self.invoke(Invocation(message, address=address))

    def invoke_smart(self, invocation, ignore_heartbeat=False):
        if invocation.has_connection():
            self._send(invocation, invocation.connection, ignore_heartbeat)
        elif invocation.has_partition_id():
            addr = self._client.partition_service.get_partition_owner(invocation.partition_id)
            self._send_to_address(invocation, addr)
        elif invocation.has_address():
            self._send_to_address(invocation, invocation.address)
        else:  # send to random address
            addr = self._client.load_balancer.next_address()
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
        for correlation_id, invocation in dict(self._pending).iteritems():
            if invocation.sent_connection == connection:
                self._handle_exception(invocation, cause)

        if self._client.lifecycle.is_live:
            for correlation_id, invocation in dict(self._event_handlers).iteritems():
                if invocation.sent_connection == connection and invocation.connection is None:
                    self._client.listener.re_register_listener(invocation)

    def _heartbeat_stopped(self, connection):
        for correlation_id, invocation in dict(self._pending).iteritems():
            if invocation.sent_connection == connection:
                self._handle_exception(invocation,
                                       TargetDisconnectedError("%s has stopped heart beating." % connection))

    def _remove_event_handler(self, correlation_id):
        self._event_handlers.pop(correlation_id)

    def _send_to_address(self, invocation, address, ignore_heartbeat=False):
        try:
            conn = self._client.connection_manager.connections[address]
            self._send(invocation, conn, ignore_heartbeat)
        except KeyError:
            if self._client.lifecycle.state != LIFECYCLE_STATE_CONNECTED:
                self._handle_exception(invocation, IOError("Client is not in connected state"))
            else:
                self._client.connection_manager.get_or_connect(address).continue_with(self.on_connect, invocation, ignore_heartbeat)

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

        if isinstance(invocation, ListenerInvocation):
            self._event_handlers[correlation_id] = invocation

        self.logger.debug("Sending %s to %s", message, connection)

        if not ignore_heartbeat and not connection.heartbeating:
            self._handle_exception(invocation, TargetDisconnectedError("%s has stopped heart beating." % connection))
            return

        invocation.sent_connection = connection
        try:
            connection.send_message(message)
        except IOError as e:
            self._handle_exception(invocation, e)

    def _handle_client_message(self, message):
        correlation_id = message.get_correlation_id()
        if message.has_flags(LISTENER_FLAG):
            if correlation_id not in self._event_handlers:
                self.logger.warn("Got event message with unknown correlation id: %s", message)
                return
            invocation = self._event_handlers[correlation_id]
            self._handle_event(invocation, message)
            return
        if correlation_id not in self._pending:
            self.logger.warn("Got message with unknown correlation id: %s", message)
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
            self.logger.warn("Error handling event %s", message, exc_info=True)

    def _handle_exception(self, invocation, error, traceback=None):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Got exception for request %s: %s: %s", invocation.request,
                              type(error).__name__, error)
        if isinstance(error, (AuthenticationError, IOError, HazelcastInstanceNotActiveError)):
            if self._try_retry(invocation):
                return

        if is_retryable_error(error):
            if invocation.request.is_retryable() or self._is_redo_operation:
                if self._try_retry(invocation):
                    return

        invocation.set_exception(error, traceback)

    def _try_retry(self, invocation):
        if invocation.connection:
            return False
        if invocation.timeout < time.time():
            return False

        invoke_func = functools.partial(self.invoke, invocation)
        self.logger.debug("Rescheduling request %s to be retried in %s seconds", invocation.request,
                          RETRY_WAIT_TIME_IN_SECONDS)
        self._client.reactor.add_timer(RETRY_WAIT_TIME_IN_SECONDS, invoke_func)
        return True
