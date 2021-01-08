import logging
import threading
from uuid import uuid4

from hazelcast import six
from hazelcast.errors import HazelcastError, HazelcastClientNotActiveError, TargetDisconnectedError
from hazelcast.future import combine_futures, ImmediateFuture
from hazelcast.invocation import Invocation
from hazelcast.protocol.codec import client_add_cluster_view_listener_codec
from hazelcast.util import check_not_none

_logger = logging.getLogger(__name__)


class _ListenerRegistration(object):
    __slots__ = (
        "registration_request",
        "decode_register_response",
        "encode_deregister_request",
        "handler",
        "connection_registrations",
    )

    def __init__(
        self, registration_request, decode_register_response, encode_deregister_request, handler
    ):
        self.registration_request = registration_request
        self.decode_register_response = decode_register_response
        self.encode_deregister_request = encode_deregister_request
        self.handler = handler
        self.connection_registrations = {}  # Dict of Connection, EventRegistration


class _EventRegistration(object):
    __slots__ = ("server_registration_id", "correlation_id")

    def __init__(self, server_registration_id, correlation_id):
        self.server_registration_id = server_registration_id
        self.correlation_id = correlation_id


class ListenerService(object):
    def __init__(self, client, config, connection_manager, invocation_service):
        self._client = client
        self._connection_manager = connection_manager
        self._invocation_service = invocation_service
        self._is_smart = config.smart_routing
        self._active_registrations = {}  # Dict of user_registration_id, ListenerRegistration
        self._registration_lock = threading.RLock()
        self._event_handlers = {}

    def start(self):
        self._connection_manager.add_listener(self._connection_added, self._connection_removed)

    def register_listener(
        self, registration_request, decode_register_response, encode_deregister_request, handler
    ):
        with self._registration_lock:
            registration_id = str(uuid4())
            registration = _ListenerRegistration(
                registration_request, decode_register_response, encode_deregister_request, handler
            )
            self._active_registrations[registration_id] = registration

            futures = []
            for connection in list(six.itervalues(self._connection_manager.active_connections)):
                future = self._register_on_connection(registration_id, registration, connection)
                futures.append(future)

            def handler(f):
                try:
                    f.result()
                    return registration_id
                except:
                    self.deregister_listener(registration_id)
                    raise HazelcastError("Listener cannot be added")

            return combine_futures(futures).continue_with(handler)

    def deregister_listener(self, user_registration_id):
        check_not_none(user_registration_id, "None user_registration_id is not allowed!")

        with self._registration_lock:
            listener_registration = self._active_registrations.pop(user_registration_id, None)
            if not listener_registration:
                return ImmediateFuture(False)

            for connection, event_registration in six.iteritems(
                listener_registration.connection_registrations
            ):
                # Remove local handler
                self.remove_event_handler(event_registration.correlation_id)
                # The rest is for deleting the remote registration
                server_registration_id = event_registration.server_registration_id
                deregister_request = listener_registration.encode_deregister_request(
                    server_registration_id
                )
                if deregister_request is None:
                    # None means no remote registration (e.g. for backup acks)
                    continue

                invocation = Invocation(
                    deregister_request, connection=connection, timeout=six.MAXSIZE, urgent=True
                )
                self._invocation_service.invoke(invocation)

                def handler(f, connection=connection):
                    e = f.exception()
                    if e:
                        if isinstance(
                            e, (HazelcastClientNotActiveError, IOError, TargetDisconnectedError)
                        ):
                            return

                        _logger.warning(
                            "Deregistration of listener with ID %s has failed for address %s",
                            user_registration_id,
                            connection.remote_address,
                        )

                invocation.future.add_done_callback(handler)

            listener_registration.connection_registrations.clear()
            return ImmediateFuture(True)

    def handle_client_message(self, message, correlation_id):
        handler = self._event_handlers.get(correlation_id, None)
        if handler:
            handler(message)
        else:
            _logger.debug("Got event message with unknown correlation id: %s", message)

    def add_event_handler(self, correlation_id, event_handler):
        self._event_handlers[correlation_id] = event_handler

    def remove_event_handler(self, correlation_id):
        self._event_handlers.pop(correlation_id, None)

    def _register_on_connection(self, user_registration_id, listener_registration, connection):
        registration_map = listener_registration.connection_registrations

        if connection in registration_map:
            return

        registration_request = listener_registration.registration_request.copy()
        invocation = Invocation(
            registration_request,
            connection=connection,
            event_handler=listener_registration.handler,
            response_handler=lambda m: m,
            urgent=True,
        )
        self._invocation_service.invoke(invocation)

        def callback(f):
            try:
                response = f.result()
                server_registration_id = listener_registration.decode_register_response(response)
                correlation_id = registration_request.get_correlation_id()
                registration = _EventRegistration(server_registration_id, correlation_id)
                registration_map[connection] = registration
            except Exception as e:
                if connection.live:
                    _logger.exception(
                        "Listener %s can not be added to a new connection: %s",
                        user_registration_id,
                        connection,
                    )
                raise e

        return invocation.future.continue_with(callback)

    def _connection_added(self, connection):
        with self._registration_lock:
            for user_reg_id, listener_registration in six.iteritems(self._active_registrations):
                self._register_on_connection(user_reg_id, listener_registration, connection)

    def _connection_removed(self, connection, _):
        with self._registration_lock:
            for listener_registration in six.itervalues(self._active_registrations):
                event_registration = listener_registration.connection_registrations.pop(
                    connection, None
                )
                if event_registration:
                    self.remove_event_handler(event_registration.correlation_id)


class ClusterViewListenerService(object):
    def __init__(
        self, client, connection_manager, partition_service, cluster_service, invocation_service
    ):
        self._client = client
        self._partition_service = partition_service
        self._connection_manager = connection_manager
        self._cluster_service = cluster_service
        self._invocation_service = invocation_service
        self._listener_added_connection = None

    def start(self):
        self._connection_manager.add_listener(self._connection_added, self._connection_removed)

    def _connection_added(self, connection):
        self._try_register(connection)

    def _connection_removed(self, connection, _):
        self._try_register_to_random_connection(connection)

    def _try_register_to_random_connection(self, old_connection):
        if self._listener_added_connection is not old_connection:
            return
        self._listener_added_connection = None
        new_connection = self._connection_manager.get_random_connection()
        if new_connection:
            self._try_register(new_connection)

    def _try_register(self, connection):
        if self._listener_added_connection:
            return

        self._cluster_service.clear_member_list_version()
        self._listener_added_connection = connection
        request = client_add_cluster_view_listener_codec.encode_request()
        invocation = Invocation(
            request, connection=connection, event_handler=self._handler(connection), urgent=True
        )
        self._cluster_service.clear_member_list_version()
        self._invocation_service.invoke(invocation)

        def callback(f):
            try:
                f.result()
            except:
                self._try_register_to_random_connection(connection)

        invocation.future.add_done_callback(callback)

    def _handler(self, connection):
        def handle_partitions_view_event(version, partitions):
            self._partition_service.handle_partitions_view_event(connection, partitions, version)

        def handle_members_view_event(member_list_version, member_infos):
            self._cluster_service.handle_members_view_event(member_list_version, member_infos)

        def inner(message):
            client_add_cluster_view_listener_codec.handle(
                message, handle_members_view_event, handle_partitions_view_event
            )

        return inner
