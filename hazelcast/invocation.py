import logging
import threading
import time
import functools

from hazelcast.exception import create_exception, HazelcastInstanceNotActiveError, is_retryable_error, TimeoutError, \
    AuthenticationError, TargetDisconnectedError, HazelcastClientNotActiveException, TargetNotMemberError, \
    HazelcastError, OperationTimeoutError
from hazelcast.future import Future
from hazelcast.lifecycle import LIFECYCLE_STATE_CONNECTED
from hazelcast.protocol.client_message import LISTENER_FLAG
from hazelcast.protocol.custom_codec import EXCEPTION_MESSAGE_TYPE, ErrorCodec
from hazelcast.util import AtomicInteger, check_not_none, current_time_in_millis
from hazelcast.six.moves import queue
from hazelcast import six
from uuid import uuid4

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


class ListenerRegistration(object):
    def __init__(self, registration_request, decode_register_response, encode_deregister_request, handler):
        self.registration_request = registration_request #of type ClientMessage (already encoded)
        self.decode_register_response = decode_register_response #take ClientMessage return String
        self.encode_deregister_request = encode_deregister_request #take String return ClientMessage
        self.handler = handler
        self.connection_registrations = {} #Map <Connection, Event Registration>)


class EventRegistration(object):
    def __init__(self, server_registration_id, correlation_id):
        self.server_registration_id = server_registration_id
        self.correlation_id = correlation_id
        # self.connection = connection --> buna gerek yok bence

"""
class ListenerInvocation(Invocation):
    # used for storing the original registration id across re-registrations
    registration_id = None

    def __init__(self, invocation_service, request, event_handler, response_decoder=None, **kwargs):
        Invocation.__init__(self, invocation_service, request, **kwargs)
        self.event_handler = event_handler
        self.response_decoder = response_decoder
"""


class ListenerService(object):
    logger = logging.getLogger("HazelcastClient.ListenerService")

    def __init__(self, client):
        self._client = client
        self._invocation_service = client.invoker
        self.is_smart = client.config.network_config.smart_routing
        self._logger_extras = {"client_name": client.name, "group_name": client.config.group_config.name}
        self.active_registrations = {}  # Map <user_registration_id, listener_registration >
        self._registration_lock = threading.RLock()
        self._event_handlers = {}

    def try_sync_connect_to_all_members(self):
        cluster_service = self._client.cluster
        start_millis = current_time_in_millis()
        while True:
            last_failed_member = None
            last_exception = None
            for member in cluster_service.members():
                try:
                    self._client.connection_manager.get_or_connect(member.address).result() #!
                except Exception as e:
                    last_failed_member = member
                    last_exception = e
            if last_exception is None:
                break
            self.time_out_or_sleep_before_next_try(start_millis, last_failed_member, last_exception)
            if not self._client.lifecycle.is_live():
                break

    def time_out_or_sleep_before_next_try(self, start_millis, last_failed_member, last_exception):
        now_in_millis = current_time_in_millis()
        elapsed_millis = now_in_millis - start_millis
        invocation_time_out_millis = self._invocation_service.invocation_timeout * 1000
        timed_out = elapsed_millis > invocation_time_out_millis
        if timed_out:
            raise OperationTimeoutError("Registering listeners is timed out."
                                        + " Last failed member : " + str(last_failed_member) + ", "
                                        + " Current time: " + str(now_in_millis) + ", "
                                        + " Start time : " + str(start_millis) + ", "
                                        + " Client invocation timeout : " + str(invocation_time_out_millis) + " ms, "
                                        + " Elapsed time : " + str(elapsed_millis) + " ms. ", last_exception)
        else:
            self.sleep_before_next_try()

    def sleep_before_next_try(self):
        self._invocation_service.invocation_retry_pause()
        pass

    def register_listener(self, registration_request, decode_register_response, encode_deregister_request, handler):
        # From JAVA: assert (!Thread.currentThread().getName().contains("eventRegistration"));
        if self.is_smart:
            self.try_sync_connect_to_all_members()

        with self._registration_lock:
            user_registration_id = str(uuid4())
            listener_registration = ListenerRegistration(registration_request, decode_register_response,
                                                         encode_deregister_request, handler)
            self.active_registrations.put(user_registration_id, listener_registration)

            active_connections = self._client.connection_manager.connections
            for connection in active_connections:
                try:
                    self.register_listener_on_connection(listener_registration, connection)
                except Exception as e:
                    if connection.live():
                        self.deregister_listener(user_registration_id)
                        raise HazelcastError("Listener cannot be added ")  # cause'unu include etmeli miyim nasil (,e)
            return user_registration_id

    def register_listener_on_connection(self, listener_registration, connection):
        # From JAVA: assert(Thread.currentThread().getName().contains("eventRegistration"));
        registration_map = listener_registration.connection_registrations

        if connection in registration_map:
            return

        registration_request = listener_registration.registration_request
        future = self._invocation_service.invoke_on_connection(registration_request, connection,
                                                                      event_handler=listener_registration.handler)
        response = future.result()

        server_registration_id = listener_registration.decode_register_response(response)
        correlation_id = registration_request.get_correlation_id()
        registration = EventRegistration(server_registration_id, correlation_id)
        registration_map.put(connection, registration)

    def deregister_listener(self, user_registration_id):
        # From JAVA: assert (!Thread.currentThread().getName().contains("eventRegistration"));
        check_not_none(user_registration_id, "Null userRegistrationId is not allowed!")

        with self._registration_lock:
            listener_registration = self.active_registrations.get(user_registration_id)
            if listener_registration is None:
                return False
            successful = True
            for connection, event_registration in listener_registration.connection_registrations.items():
                try:
                    server_registration_id = event_registration.server_registration_id
                    deregister_request = listener_registration.encode_deregister_request(server_registration_id)
                    self._invocation_service.invoke_on_connection(deregister_request, connection).result()
                    self.remove_event_handler(event_registration.correlation_id)
                    listener_registration.connection_registrations.pop(connection)
                except Exception:
                    if connection.live():
                        successful = False
                        self.logger.warning("Deregistration for listener with ID {} has failed to address {} ".format
                                            (user_registration_id, "address"), exc_info=True, extra=self._logger_extras)
            if successful:
                self.active_registrations.pop(user_registration_id)
            return successful

    def connection_added(self, connection):
        # From JAVA: assert (!Thread.currentThread().getName().contains("eventRegistration"));
        with self._registration_lock:
            for listener_registration in self.active_registrations.values():
                self.register_listener_on_connection(listener_registration, connection)

    def connection_removed(self, connection):
        # From JAVA: assert (!Thread.currentThread().getName().contains("eventRegistration"));
        with self._registration_lock:
            for listener_registration in self.active_registrations.values():
                event_registration = listener_registration.connection_registrations.pop(connection)
                if event_registration is not None:
                    self.remove_event_handler(event_registration.correlation_id)

    def start(self):  # ?
        # clientConnectionManager.addConnectionListener(this);
        # ama pythonda add_listener parametre olarak listener almiyor.
        self._client.connection_manager.add_listener(self.connection_added, self.connection_removed)
        # .NET'te bu var gerekli mi:
        # if (IsSmart)
        # {
        #     _connectionReopener = new Timer(ReOpenAllConnectionsIfNotOpen, null, 1000, 1000);
        # }

    def handle_client_message(self, message):
        correlation_id = message.get_correlation_id()
        if correlation_id not in self._event_handlers:
            self.logger.warning("Got event message with unknown correlation id: %s", message, extra=self._logger_extras)
        event_handler = self._event_handlers.get(correlation_id)
        event_handler(message)
        # Asagidakini yapmak yerine bu sekilde yaptim
        # invocation = self._event_handlers[correlation_id] --> bundan nasil invocation cikabiliyor ki?
        # self._handle_event(invocation, message)

    def add_event_handler(self, correlation_id, event_handler):
        self._event_handlers[correlation_id] = event_handler

    def remove_event_handler(self, correlation_id):
        self._event_handlers.pop(correlation_id)

    """
    def start_listening(self, request, event_handler, decode_add_listener, key=None):
        if key:
            partition_id = self._client.partition_service.get_partition_id(key)
            invocation = ListenerInvocation(self, request, event_handler, decode_add_listener,
                                            partition_id=partition_id)
        else:
            invocation = ListenerInvocation(self, request, event_handler, decode_add_listener)

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
    """
    """
    # Gerekli mi?
    def re_register_listener(self, invocation):
        registration_id = invocation.registration_id
        new_invocation = ListenerInvocation(self, invocation.request, invocation.event_handler,
                                            invocation.response_decoder, partition_id=invocation.partition_id)
        new_invocation.registration_id = registration_id

        # re-send the request
        def callback(f):
            if f.is_success():
                new_id = new_invocation.response_decoder(f.result())
                self.logger.debug("Re-registered listener with id %s and new_id %s for request %s",
                                  registration_id, new_id, new_invocation.request, extra=self._logger_extras)
                self.registrations[registration_id] = (new_id, new_invocation.request.get_correlation_id())
            else:
                self.logger.warning("Re-registration for listener with id %s failed.", registration_id, exc_info=True,
                                    extra=self._logger_extras)

        self.logger.debug("Re-registering listener %s for request %s", registration_id, new_invocation.request,
                          extra=self._logger_extras)
        self._client.invoker.invoke(new_invocation).add_done_callback(callback)
    """
    """
    # Gerekli mi? Bunun aynisi InvocationService'te de var.
    def _init_invocation_timeout(self):
        invocation_timeout = self._client.properties.get_seconds_positive_or_default(
            self._client.properties.INVOCATION_TIMEOUT_SECONDS)
        return invocation_timeout
    """


class InvocationService(object):
    logger = logging.getLogger("HazelcastClient.InvocationService")

    def __init__(self, client):
        self._pending = {}
        self._next_correlation_id = AtomicInteger(1)
        self._client = client
        self._logger_extras = {"client_name": client.name, "group_name": client.config.group_config.name}
        self._event_queue = queue.Queue()
        self._is_redo_operation = client.config.network_config.redo_operation
        self.invocation_retry_pause = self._init_invocation_retry_pause() #eskiden private'ti public yaptim.
        self.invocation_timeout = self._init_invocation_timeout()
        self._listener_service = client.listener

        if client.config.network_config.smart_routing:
            self.invoke = self.invoke_smart
        else:
            self.invoke = self.invoke_non_smart

        self._client.connection_manager.add_listener(on_connection_closed=self.cleanup_connection)
        client.heartbeat.add_listener(on_heartbeat_stopped=self._heartbeat_stopped)

    def invoke_on_connection(self, message, connection, ignore_heartbeat=False, event_handler=None):
        return self.invoke(Invocation(self, message, connection=connection, event_handler=event_handler), ignore_heartbeat)

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
                self._handle_exception(invocation, TargetNotMemberError("Target '{}' is not a member.".format(invocation.address)))
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

        if self._client.lifecycle.is_live:
            for correlation_id, invocation in six.iteritems(dict(self._event_handlers)):
                if invocation.sent_connection == connection and invocation.connection is None:
                    self._client.listener.re_register_listener(invocation)

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
    """
    def _remove_event_handler(self, correlation_id):
        self._event_handlers.pop(correlation_id)
    """

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
            self._listener_service.remove_event_handler(correlation_id)  # fail ederse event handleri temizlemek icin
            self._handle_exception(invocation, e)

    def _handle_client_message(self, message):
        correlation_id = message.get_correlation_id()
        if message.has_flags(LISTENER_FLAG):
            self._listener_service.handle_client_message(message)
            # if correlation_id not in self._event_handlers:
            #     self.logger.warning("Got event message with unknown correlation id: %s", message,
            #                         extra=self._logger_extras)
            #     return
            # invocation = self._event_handlers[correlation_id]
            # self._handle_event(invocation, message)
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
        if not self._client.lifecycle.is_live:
            invocation.set_exception(HazelcastClientNotActiveException(error.args[0]), traceback)
            return
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Got exception for request %s: %s: %s", invocation.request, type(error).__name__, error,
                              extra=self._logger_extras)
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
                          self.invocation_retry_pause, extra=self._logger_extras)
        self._client.reactor.add_timer(self.invocation_retry_pause, invoke_func)
        return True

    def _is_member(self, address):
        return self._client.cluster.get_member_by_address(address) is not None
