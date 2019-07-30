import logging
import threading
from uuid import uuid4

from hazelcast.exception import OperationTimeoutError, HazelcastError
from hazelcast.util import current_time_in_millis, check_not_none


class ListenerRegistration(object):
    def __init__(self, registration_request, decode_register_response, encode_deregister_request, handler):
        self.registration_request = registration_request
        self.decode_register_response = decode_register_response
        self.encode_deregister_request = encode_deregister_request
        self.handler = handler
        self.connection_registrations = {} #Map <Connection, Event Registration>)


class EventRegistration(object):
    def __init__(self, server_registration_id, correlation_id):
        self.server_registration_id = server_registration_id
        self.correlation_id = correlation_id


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
            for member in cluster_service.members:
                try:
                    self._client.connection_manager.get_or_connect(member.address).result()
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
            self._invocation_service.invocation_retry_pause()  # sleep before next try

    def register_listener(self, registration_request, decode_register_response, encode_deregister_request, handler):
        if self.is_smart:
            self.try_sync_connect_to_all_members()

        with self._registration_lock:
            user_registration_id = str(uuid4())
            listener_registration = ListenerRegistration(registration_request, decode_register_response,
                                                         encode_deregister_request, handler)
            self.active_registrations[user_registration_id] = listener_registration

            active_connections = self._client.connection_manager.connections
            for connection in active_connections.values():
                try:
                    self.register_listener_on_connection(listener_registration, connection)
                except:
                    if connection.live():
                        self.deregister_listener(user_registration_id)
                        raise HazelcastError("Listener cannot be added ")  # cause'unu include etmeli miyim nasil (,e)
            return user_registration_id

    def register_listener_on_connection(self, listener_registration, connection):
        registration_map = listener_registration.connection_registrations

        if connection in registration_map:
            return

        registration_request = listener_registration.registration_request
        future = self._invocation_service.invoke_on_connection(registration_request, connection,
                                                               event_handler=listener_registration.handler)

        def callback(f):
            try:
                response = f.result()
                server_registration_id = listener_registration.decode_register_response(response)
                correlation_id = registration_request.get_correlation_id()
                registration = EventRegistration(server_registration_id, correlation_id)
                registration_map[connection] = registration
            except:
                # Gotta handle exception
                pass

        future.add_done_callback(callback)

    def deregister_listener(self, user_registration_id):
        check_not_none(user_registration_id, "Null userRegistrationId is not allowed!")

        with self._registration_lock:
            listener_registration = self.active_registrations.get(user_registration_id)
            if listener_registration is None:
                return False
            successful = True
            for connection, event_registration in list(listener_registration.connection_registrations.items()):
                try:
                    server_registration_id = event_registration.server_registration_id
                    deregister_request = listener_registration.encode_deregister_request(server_registration_id)
                    self._invocation_service.invoke_on_connection(deregister_request, connection).result()
                    self.remove_event_handler(event_registration.correlation_id)
                    listener_registration.connection_registrations.pop(connection)
                except:
                    if connection.live():
                        successful = False
                        self.logger.warning("Deregistration for listener with ID {} has failed to address {} ".format
                                            (user_registration_id, "address"), exc_info=True, extra=self._logger_extras)
            if successful:
                self.active_registrations.pop(user_registration_id)
            return successful

    def connection_added(self, connection):
        with self._registration_lock:
            for listener_registration in self.active_registrations.values():
                self.register_listener_on_connection(listener_registration, connection)

    def connection_removed(self, connection, _):
        with self._registration_lock:
            for listener_registration in self.active_registrations.values():
                event_registration = listener_registration.connection_registrations.pop(connection, None)
                if event_registration is not None:
                    self.remove_event_handler(event_registration.correlation_id)

    def start(self):
        self._client.connection_manager.add_listener(self.connection_added, self.connection_removed)

    def handle_client_message(self, message):
        correlation_id = message.get_correlation_id()
        if correlation_id not in self._event_handlers:
            self.logger.warning("Got event message with unknown correlation id: %s", message, extra=self._logger_extras)
        event_handler = self._event_handlers.get(correlation_id)
        event_handler(message)

    def add_event_handler(self, correlation_id, event_handler):
        self._event_handlers[correlation_id] = event_handler

    def remove_event_handler(self, correlation_id):
        self._event_handlers.pop(correlation_id, None)