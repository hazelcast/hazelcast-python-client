import logging
import struct
import sys
import threading
import time
import random

from hazelcast.exception import AuthenticationError, HazelcastIllegalStateError, ClientNotAllowedInClusterError
from hazelcast.future import ImmediateFuture, ImmediateExceptionFuture, Future
from hazelcast.protocol.client_message import ClientMessage, ClientMessageBuilder
from hazelcast.protocol.codec import client_authentication_codec, client_ping_codec
from hazelcast.serialization import INT_SIZE_IN_BYTES, FMT_LE_INT
from hazelcast.util import AtomicInteger, parse_addresses, calculate_version, enum, get_provider_addresses, \
    get_possible_addresses
from hazelcast.version import CLIENT_TYPE, CLIENT_VERSION, SERIALIZATION_VERSION
from hazelcast import six
from hazelcast.protocol.client_message_writer import ClientMessageWriter
from hazelcast.lifecycle import LIFECYCLE_STATE_CLIENT_CONNECTED, LIFECYCLE_STATE_CLIENT_DISCONNECTED
from hazelcast.exception import HazelcastClientNotActiveException, TargetDisconnectedError

BUFFER_SIZE = 128000
PROTOCOL_VERSION = 1
MAX_INT_VALUE = 0x7fffffff

AUTHENTICATION_STATUS = enum(AUTHENTICATED=0, CREDENTIALS_FAILED=1, SERIALIZATION_VERSION_MISMATCH=2,
                             NOT_ALLOWED_IN_CLUSTER=3)

CLIENT_STATE = enum(INITIAL=0, CONNECTED_TO_CLUSTER=1, INITIALIZED_ON_CLUSTER=2)


class ConnectionManager(object):
    """
    ConnectionManager is responsible for managing :mod:`Connection` objects.
    """
    logger = logging.getLogger("HazelcastClient.ConnectionManager")
    cluster_id = None

    def __init__(self, client, new_connection_func):
        self._new_connection_mutex = threading.RLock()
        self._io_thread = None
        self._client = client
        self.client_uuid = client.uuid
        self._load_balancer = client.load_balancer
        self._connection_timeout_millis = self.init_connection_timeout_millis()
        self.heartbeat_manager = HeartbeatManager(client, self)
        self.connections = {}
        self._pending_connections = {}
        self._new_connection_func = new_connection_func
        self._connection_listeners = []
        self._logger_extras = {"client_name": client.name, "group_name": client.config.cluster_name}
        self.is_smart_routing_enabled = self._client.config.network_config.smart_routing
        self.wait_strategy = self._initialize_wait_strategy(client.config)
        self.connection_strategy_config = self._client.config.connection_strategy_config
        self.async_start = self.connection_strategy_config.async_start
        self.reconnect_mode = self.connection_strategy_config.reconnect_mode
        self.client_state = CLIENT_STATE.INITIAL
        self.alive = False
        self.connect_to_cluster_task_submitted = False
        self.connect_to_all_cluster_members_task = None

    def start(self):
        """
        Connects to cluster.
        """
        self.alive = True

        def callback(f):
            if f.result() and self.is_smart_routing_enabled:
                self.heartbeat_manager.start()
                self.connect_to_all_cluster_members_task = self._client.reactor. \
                    add_timer(1, self.connect_to_all_cluster_members)
                return

        self.connect_to_cluster().add_done_callback(callback)

    def add_listener(self, on_connection_opened=None, on_connection_closed=None):
        """
        Registers a ConnectionListener. If the same listener is registered multiple times, it will be notified multiple
        times.

        :param on_connection_opened: (Function), function to be called when a connection is opened.
        :param on_connection_closed: (Function), function to be called when a connection is removed.
        """
        self._connection_listeners.append((on_connection_opened, on_connection_closed))

    def get_connection_by_address(self, address):
        """
        Gets the existing connection for a given address or connects. This call is silent.

        :param address: (:class:`~hazelcast.core.Address`), the address to connect to.
        :return: (:class:`~hazelcast.connection.Connection`), the found connection, or None if no connection exists.
        """
        for connection in self.connections.values():
            if connection.remote_address == address:
                return connection
        return None

    def get_connection(self, uuid):
        """
        Gets the existing connection for a given uuid.

        :param uuid: (:class:`uuid.UUID`), the uuid to connect to.
        :return: (:class:`~hazelcast.connection.Connection`), the found connection, or None if no connection exists.
        """
        try:
            return self.connections[uuid]
        except KeyError:
            return None

    def get_random_connection(self):
        if self._client.config.network_config.smart_routing:
            if self.connections:
                return random.choice(list(self.connections.items()))[1]
            else:
                return None

    def _cluster_authenticator(self, connection):
        request = client_authentication_codec.encode_request(
            cluster_name=self._client.config.cluster_name,
            username=None,
            password=None,
            uuid=self.client_uuid,
            client_type=CLIENT_TYPE,
            serialization_version=SERIALIZATION_VERSION,
            client_hazelcast_version=CLIENT_VERSION,
            client_name=self._client.name,
            labels=[])

        def callback(f):
            parameters = client_authentication_codec.decode_response(f.result())
            if parameters["status"] != 0:
                raise AuthenticationError("Authentication failed.")
            connection.remote_address = parameters["address"]
            connection.remote_uuid = parameters["memberUuid"]
            connection.server_version_str = parameters.get("server_hazelcast_version", "")
            connection.server_version = calculate_version(connection.server_version_str)
            return connection

        return self._client.invoker.invoke_on_connection(request, connection).continue_with(callback)

    def get_or_connect(self, address, authenticator=None):
        """
        Gets the existing connection for a given address. If it does not exist, the system will try to connect
        asynchronously. In this case, it returns a Future. When the connection is established at some point in time, it
        can be retrieved by using the get_connection(:class:`~hazelcast.core.Address`) or from Future.

        :param address: (:class:`~hazelcast.core.Address`), the address to connect to.
        :param authenticator: (Function), function to be used for authentication (optional).
        :return: (:class:`~hazelcast.connection.Connection`), the existing connection or it returns a Future which
        includes asynchronously.
        """
        if address in [connection.remote_address for connection in self.connections.values()]:
            return ImmediateFuture(self.connections[self.get_connection_by_address(address).remote_uuid])
        else:
            with self._new_connection_mutex:
                if address in self._pending_connections:
                    return self._pending_connections[address]
                else:
                    authenticator = authenticator or self._cluster_authenticator
                    try:
                        translated_address = address
                        if translated_address is None:
                            raise ValueError("Address translator could not translate address: {}".format(address))
                        connection = self._new_connection_func(self._client,
                                                               translated_address,
                                                               self._client.config.network_config.connection_timeout,
                                                               self._client.config.network_config.socket_options,
                                                               connection_closed_callback=self._connection_closed,
                                                               message_callback=self._client.invoker
                                                               .handle_client_message,
                                                               network_config=self._client.config.network_config)
                    except IOError:
                        return ImmediateExceptionFuture(sys.exc_info()[1], sys.exc_info()[2])

                    future = authenticator(connection).continue_with(self.on_auth, connection, address)
                    if not future.done():
                        self._pending_connections[address] = future
                    return future

    def on_auth(self, f, connection, address):
        """
        Checks for authentication of a connection.

        :param f: (:class:`~hazelcast.future.Future`), future that contains the result of authentication.
        :param connection: (:class:`~hazelcast.connection.Connection`), newly established connection.
        :param address: (:class:`~hazelcast.core.Address`), the adress of new connection.
        :return: Result of authentication.
        """
        if f.is_success():
            self.logger.info("Authenticated with %s", f.result(), extra=self._logger_extras)
            with self._new_connection_mutex:
                self.connections[connection.remote_uuid] = f.result()
                try:
                    self._pending_connections.pop(address)
                except KeyError:
                    pass
            for on_connection_opened, _ in self._connection_listeners:
                if on_connection_opened:
                    on_connection_opened(f.result())
            return f.result()
        else:
            self.logger.debug("Error opening %s", connection, extra=self._logger_extras)
            with self._new_connection_mutex:
                try:
                    self._pending_connections.pop(address)
                except KeyError:
                    pass
            six.reraise(f.exception().__class__, f.exception(), f.traceback())

    def _connection_closed(self, connection, cause):
        # if connection was authenticated, fire event
        if connection.remote_address:
            try:
                self.connections.pop(connection.remote_address)
            except KeyError:
                pass
            for _, on_connection_closed in self._connection_listeners:
                if on_connection_closed:
                    on_connection_closed(connection, cause)
        else:
            # clean-up unauthenticated connection
            self._client.invoker.cleanup_connection(connection, cause)

    def close_connection(self, address, cause):
        """
        Closes the connection with given address.

        :param address: (:class:`~hazelcast.core.Address`), address of the connection to be closed.
        :param cause: (Exception), the cause for closing the connection.
        :return: (bool), ``true`` if the connection is closed, ``false`` otherwise.
        """
        try:
            connection = self.connections[address]
            connection.close(cause)
        except KeyError:
            self.logger.warning("No connection with %s was found to close.", address, extra=self._logger_extras)
            return False

    def on_connection_close(self, connection):
        member_address = connection.remote_address
        member_uuid = connection.remote_uuid

        if member_address is None:
            self.logger.debug('Destroying {}, but it has end-point set to null -> not removing it from connection map'
                              .format(connection), extra=self._logger_extras)
            return

        if member_uuid is not None and member_uuid in self.connections.keys():
            self.connections.pop(member_uuid)
            self.logger.info('Removed connection to endpoint: {}: {}, connection: '
                             .format(member_address, member_uuid, connection))
            if not self.connections:
                if self.client_state == CLIENT_STATE.INITIALIZED_ON_CLUSTER:
                    self._client.lifecycle.fire_lifecycle_event(LIFECYCLE_STATE_CLIENT_DISCONNECTED)

    def connect_to_cluster(self):
        if self.async_start:
            return self.submit_connect_to_cluster_task()
        else:
            return self._do_connect_to_cluster()

    def _initialize_wait_strategy(self, config):
        connection_strategy_config = config.connection_strategy_config
        expo_retry_config = connection_strategy_config.connection_retry_config

        return WaitStrategy(expo_retry_config.initial_backoff_millis,
                            expo_retry_config.max_backoff_millis,
                            expo_retry_config.multiplier,
                            expo_retry_config.connect_timeout_millis,
                            expo_retry_config.jitter, self.logger)

    def submit_connect_to_cluster_task(self):
        future = Future()

        if self.connect_to_cluster_task_submitted:
            future.set_result(True)
            return future

        def callback(f):
            if not f.result():
                self.connect_to_cluster_task_submitted = False
                return self.submit_connect_to_cluster_task()

        self._do_connect_to_cluster().continue_with(callback)

        self.connect_to_cluster_task_submitted = True
        future.set_result(True)
        return future

    def _do_connect_to_cluster(self):
        future = Future()
        current_attempt = 1
        self.wait_strategy.reset()
        while True:
            provider_addresses = get_provider_addresses(self._client.create_address_providers())
            addresses = get_possible_addresses(provider_addresses, self._client.cluster.get_member_list())

            for address in addresses:
                try:
                    self.check_client_active()
                    self.logger.info("Connecting to %s", address, extra=self._logger_extras)
                    future = self._connect_to_address(address)
                    future.set_result(True)
                    return future

                except:
                    self.logger.warning("Error connecting to %s ", address, exc_info=True, extra=self._logger_extras)
                    future.set_result(False)

            current_attempt += 1
            if not self._client.connection_manager.wait_strategy.sleep():
                break

        self.shutdown_client()
        future.set_result(False)
        return future

    def _connect_to_address(self, address):
        f = self.get_or_connect(address, self._authenticate_manager)
        connection = f.result()
        if not connection.is_owner:
            self._authenticate_manager(connection).result()
        self.owner_connection_address = connection.remote_address
        # self._init_membership_listener(connection)
        self._client.lifecycle.fire_lifecycle_event(LIFECYCLE_STATE_CLIENT_CONNECTED)
        return f

    def _authenticate_manager(self, connection):
        request = client_authentication_codec.encode_request(
            cluster_name=self._client.config.cluster_name,
            username=None,
            password=None,
            uuid=self.client_uuid,
            client_type=CLIENT_TYPE,
            serialization_version=SERIALIZATION_VERSION,
            client_hazelcast_version=CLIENT_VERSION,
            client_name=self._client.name,
            labels=[])

        def callback(f):
            parameters = client_authentication_codec.decode_response(f.result())

            if parameters["status"] == AUTHENTICATION_STATUS.AUTHENTICATED:
                return self.handle_successful_auth(connection, parameters)

            elif parameters["status"] == AUTHENTICATION_STATUS.CREDENTIALS_FAILED:
                raise AuthenticationError("Authentication failed. The configured cluster name on the client does not "
                                          "match the one configured in the cluster or the credentials set in the "
                                          "client security config could not be authenticated")

            elif parameters["status"] == AUTHENTICATION_STATUS.SERIALIZATION_VERSION_MISMATCH:
                raise HazelcastIllegalStateError("Server serialization version does not match to client.")

            elif parameters["status"] == AUTHENTICATION_STATUS.NOT_ALLOWED_IN_CLUSTER:
                ClientNotAllowedInClusterError("Client is not allowed in the cluster.")

        return self._client.invoker.invoke_on_connection(request, connection).continue_with(callback)

    def handle_successful_auth(self, connection, parameters):
        self.check_partition_count(parameters["partitionCount"])
        connection.remote_address = parameters["address"]
        connection.remote_uuid = parameters["memberUuid"]
        new_cluster_id = parameters["clusterId"]
        connection.is_owner = True
        initial_connection = len(self.connections) == 0
        changed_cluster = initial_connection and self.cluster_id is not None and new_cluster_id != self.cluster_id

        if changed_cluster:
            self.logger.warning("Switching from current cluster: {} to new cluster: {}"
                                .format(self.cluster_id, new_cluster_id))
            self._client.on_cluster_restart()

        # TODO: Key type of active connections might need to be str
        self.connections[parameters["memberUuid"]] = connection

        if initial_connection:
            self.cluster_id = new_cluster_id
            if changed_cluster:
                self.client_state = CLIENT_STATE.CONNECTED_TO_CLUSTER
                self.initialize_client_on_cluster(new_cluster_id)
            else:
                self.client_state = CLIENT_STATE.INITIALIZED_ON_CLUSTER
                self._client.lifecycle.fire_lifecycle_event(LIFECYCLE_STATE_CLIENT_CONNECTED)

        # TODO: Add local address
        self.logger.info("Authenticated with server {}: {}, server version: {}, local address: {}"
                         .format(parameters["address"],
                                 parameters["memberUuid"],
                                 parameters["serverHazelcastVersion"],
                                 "localhost"))
        connection.server_version_str = parameters.get("serverHazelcastVersion", "")
        connection.server_version = calculate_version(connection.server_version_str)
        return connection

    def check_client_active(self):
        if not self._client.lifecycle.is_live:
            raise HazelcastClientNotActiveException

    def shutdown_client(self):
        self._client.lifecycle.shutdown()

    def init_connection_timeout_millis(self):
        network_config = self._client.config.network_config
        conn_timeout = network_config.connection_timeout
        return MAX_INT_VALUE if conn_timeout == 0 else conn_timeout

    def check_partition_count(self, new_partition_count):
        partition_service = self._client.partition_service
        if not partition_service.check_and_set_partition_count(new_partition_count):
            raise ClientNotAllowedInClusterError("Client can not work with this cluster because it has a different "
                                                 "partition count. Expected partition count: {},"
                                                 " member partition count: {}"
                                                 .format(partition_service.partition_count, new_partition_count))

    def initialize_client_on_cluster(self, target_cluster_id):
        try:
            if target_cluster_id != self.cluster_id:
                self.logger.warning("Won't send client state to cluster: {}, Because switched to a new cluster: {}"
                                    .format(target_cluster_id, self.cluster_id), extra=self._logger_extras)
                return

            if target_cluster_id == self.cluster_id:
                self.logger.debug("Client state is sent to cluster: {}".format(target_cluster_id),
                                  extra=self._logger_extras)
                self.client_state = CLIENT_STATE.INITIALIZED_ON_CLUSTER
                self._client.lifecycle.fire_lifecycle_event(LIFECYCLE_STATE_CLIENT_CONNECTED)
            else:
                self.logger.warning("Cannot set client state to initialized on cluster because current cluster id: {}"
                                    " is different than expected cluster id: {}"
                                    .format(self.cluster_id, target_cluster_id))
        except Exception:
            cluster_name = self._client.config.cluster_name
            self.logger.warning("Failure during sending state to the cluster.")
            if target_cluster_id == self.cluster_id:
                self.logger.warning("Retrying sending state to the cluster: {}, name: {}"
                                    .format(target_cluster_id, cluster_name))
                # self.initialize_client_on_cluster(target_cluster_id)

    def get_active_connections(self):
        return self.connections.values()

    def connect_to_all_cluster_members(self):
        if not self._client.lifecycle.is_live:
            return

        for member in self._client.cluster.members:
            self.get_or_connect(member.address)

        self.connect_to_all_cluster_members_task = self._client.reactor. \
            add_timer(1, self.connect_to_all_cluster_members)

    def try_connect_to_all_cluster_members(self, members):
        for member in members:
            try:
                self.get_or_connect(member.address).result()
            except Exception as e:
                print(str(e))

    def shutdown(self):
        if not self.alive:
            return

        self.alive = False
        del self._connection_listeners[:]
        self.heartbeat_manager.shutdown()


class HeartbeatManager(object):
    """
    HeartbeatManager manager used by connection manager.
    """
    logger = logging.getLogger("HazelcastClient.HeartbeatService")
    _heartbeat_timer = None

    def __init__(self, client, connection_manager):
        self._client = client
        self._connection_manager = connection_manager
        self._logger_extras = {"client_name": client.name, "cluster_name": client.config.cluster_name}
        self._heartbeat_timeout = client.properties.get_seconds_positive_or_default(client.properties.HEARTBEAT_TIMEOUT)
        self._heartbeat_interval = client.properties.get_seconds_positive_or_default(
            client.properties.HEARTBEAT_INTERVAL)

    def start(self):
        """
        Starts sending periodic HeartBeat operations.
        """

        self._heartbeat_timer = self._client.reactor.add_timer(self._heartbeat_interval, self._heartbeat)

    def shutdown(self):
        """
        Stops HeartBeat operations.
        """
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()

    def _heartbeat(self):
        if not self._client.lifecycle.is_live:
            return
        now = time.time()
        for connection in list(self._client.connection_manager.connections.values()):
            self._check_connection(now, connection)

        self._heartbeat_timer = self._client.reactor.add_timer(self._heartbeat_interval, self._heartbeat)

    def _check_connection(self, now, connection):
        if not connection.live():
            return

        time_since_last_read = now - connection.last_read_in_seconds
        time_since_last_write = now - connection.last_write_in_seconds
        if time_since_last_read > self._heartbeat_timeout:
            if connection.live():
                self.logger.warning(
                    "Heartbeat: Heartbeat failed over the connection: {}".format(connection),
                    extra=self._logger_extras)
                HeartbeatManager._on_heartbeat_stopped(connection)

        if time_since_last_write > self._heartbeat_interval:
            request = client_ping_codec.encode_request()
            self._client.invoker.invoke_on_connection(request, connection)

    @staticmethod
    def _on_heartbeat_stopped(connection):
        connection.close(TargetDisconnectedError("Heartbeat timed out to connection {})".format(connection)))


class Connection(object):
    """
    Connection object which stores connection related information and operations.
    """
    _closed = False
    remote_address = None
    remote_uuid = None
    is_owner = False
    counter = AtomicInteger()

    def __init__(self, address, connection_closed_callback, message_callback, logger_extras=None):
        self._address = (address.host, address.port)
        self._logger_extras = logger_extras
        self.id = self.counter.get_and_increment()
        self.logger = logging.getLogger("HazelcastClient.Connection[%s](%s:%d)" % (self.id, address.host, address.port))
        self._connection_closed_callback = connection_closed_callback
        self.callback = message_callback
        self._builder = ClientMessageBuilder(message_callback)
        self.read_buffer = bytearray()
        self.last_read_in_seconds = 0
        self.last_write_in_seconds = 0
        self.start_time_in_seconds = 0
        self.server_version_str = ""
        self.server_version = 0
        self.bytes_read = 0
        self.bytes_written = 0
        self.writer = ClientMessageWriter(self)

    def live(self):
        """
        Determines whether this connection is live or not.

        :return: (bool), ``true`` if the connection is live, ``false`` otherwise.
        """
        return not self._closed

    def send_message(self, message):
        """
        Sends a message to this connection.

        :param message: (Message), message to be sent to this connection.
        """
        if not self.live():
            raise IOError("Connection is not live.")

        self.writer.write_to(message)

    def receive_message(self):
        """
        Receives a message from this connection.
        """
        # split frames
        while len(self.read_buffer) >= INT_SIZE_IN_BYTES:
            frame_length = struct.unpack_from(FMT_LE_INT, self.read_buffer, 0)[0]
            if frame_length > len(self.read_buffer):
                return
            message = ClientMessage(memoryview(self.read_buffer)[:frame_length])
            self.read_buffer = self.read_buffer[frame_length:]
            self._builder.on_message(message)

    def write(self, data):
        """
        Writes data to this connection when sending messages.

        :param data: (Data), data to be written to connection.
        """
        # must be implemented by subclass
        raise NotImplementedError

    def close(self, cause):
        """
        Closes the connection.

        :param cause: (Exception), the cause of closing the connection.
        """
        raise NotImplementedError

    def __repr__(self):
        return "Connection(address=%s, id=%s)" % (self._address, self.id)

    def __hash__(self):
        return self.id


class DefaultAddressProvider(object):
    """
    Default address provider of Hazelcast.
    Loads addresses from the Hazelcast configuration.
    """

    def __init__(self, network_config):
        self._network_config = network_config

    def load_addresses(self):
        """
        :return: (Sequence), The possible member addresses to connect to.
        """
        return parse_addresses(self._network_config.addresses)

    def translate(self, address):
        """
        :param address: (:class:`~hazelcast.core.Address`), address to be translated.
        :return: (:class:`~hazelcast.core.Address`), translated address.
        """
        return address


class WaitStrategy(object):
    attempt = 0
    random = random.random()
    cluster_connect_attempt_begin = 0
    current_backoff_millis = 0

    def __init__(self, initial_backoff_millis, max_backoff_millis, multiplier, cluster_connect_timeout_millis, jitter,
                 logger):
        self.initial_backoff_millis = initial_backoff_millis
        self.max_backoff_millis = max_backoff_millis
        self.multiplier = multiplier
        self.cluster_connect_timeout_millis = cluster_connect_timeout_millis
        self.jitter = jitter
        self.logger = logger

    def reset(self):
        self.attempt = 0
        self.cluster_connect_attempt_begin = int(round(time.time() * 1000))
        self.current_backoff_millis = min(self.max_backoff_millis, self.initial_backoff_millis)

    def sleep(self):
        self.attempt += 1
        current_time_millis = int(round(time.time() * 1000))
        time_passed = current_time_millis - self.cluster_connect_attempt_begin
        if time_passed > self.cluster_connect_timeout_millis:
            self.logger.warning("Unable to get live cluster connection, cluster connect timeout ({} millis) is reached."
                                "Attempt {}.".format(self.cluster_connect_timeout_millis, self.attempt))
            return False

        actual_sleep_time = self.current_backoff_millis - (self.current_backoff_millis * self.jitter) + \
                            (self.current_backoff_millis * self.jitter * random.random())

        actual_sleep_time = min(actual_sleep_time, self.cluster_connect_timeout_millis - time_passed)

        self.logger.warning("Unable to get live cluster connection, retry in {} ms, attempt: {}"
                            ", cluster connect timeout: {} seconds"
                            ", max backoff millis: {}".format(actual_sleep_time, self.attempt,
                                                              self.cluster_connect_timeout_millis,
                                                              self.max_backoff_millis))

        time.sleep(actual_sleep_time / 1000)

        self.current_backoff_millis = min(self.current_backoff_millis * self.multiplier, self.max_backoff_millis)

        return True
