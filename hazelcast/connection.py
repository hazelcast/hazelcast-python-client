import io
import logging
import random
import struct
import threading
import time
import uuid

from hazelcast import six, __version__
from hazelcast.config import ReconnectMode
from hazelcast.core import AddressHelper, CLIENT_TYPE, SERIALIZATION_VERSION
from hazelcast.errors import (
    AuthenticationError,
    TargetDisconnectedError,
    HazelcastClientNotActiveError,
    InvalidConfigurationError,
    ClientNotAllowedInClusterError,
    IllegalStateError,
    ClientOfflineError,
)
from hazelcast.future import ImmediateFuture, ImmediateExceptionFuture
from hazelcast.invocation import Invocation
from hazelcast.lifecycle import LifecycleState
from hazelcast.protocol.client_message import (
    SIZE_OF_FRAME_LENGTH_AND_FLAGS,
    Frame,
    InboundMessage,
    ClientMessageBuilder,
)
from hazelcast.protocol.codec import (
    client_authentication_codec,
    client_authentication_custom_codec,
    client_ping_codec,
)
from hazelcast.util import AtomicInteger, calculate_version, UNKNOWN_VERSION

_logger = logging.getLogger(__name__)

_INF = float("inf")


class _WaitStrategy(object):
    def __init__(self, initial_backoff, max_backoff, multiplier, cluster_connect_timeout, jitter):
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._multiplier = multiplier
        self._cluster_connect_timeout = cluster_connect_timeout
        self._jitter = jitter
        self._attempt = None
        self._cluster_connect_attempt_begin = None
        self._current_backoff = None

        if cluster_connect_timeout == _INF:
            self._cluster_connect_timeout_text = "INFINITE"
        else:
            self._cluster_connect_timeout_text = "%.2fs" % self._cluster_connect_timeout

    def reset(self):
        self._attempt = 0
        self._cluster_connect_attempt_begin = time.time()
        self._current_backoff = min(self._max_backoff, self._initial_backoff)

    def sleep(self):
        self._attempt += 1
        time_passed = time.time() - self._cluster_connect_attempt_begin
        if time_passed > self._cluster_connect_timeout:
            _logger.warning(
                "Unable to get live cluster connection, cluster connect timeout (%s) is reached. "
                "Attempt %d.",
                self._cluster_connect_timeout_text,
                self._attempt,
            )
            return False

        # random between (-jitter * current_backoff, jitter * current_backoff)
        sleep_time = self._current_backoff + self._current_backoff * self._jitter * (
            2 * random.random() - 1
        )
        sleep_time = min(sleep_time, self._cluster_connect_timeout - time_passed)
        _logger.warning(
            "Unable to get live cluster connection, retry in %.2fs, attempt: %d, "
            "cluster connect timeout: %s, max backoff: %.2fs",
            sleep_time,
            self._attempt,
            self._cluster_connect_timeout_text,
            self._max_backoff,
        )
        time.sleep(sleep_time)
        self._current_backoff = min(self._current_backoff * self._multiplier, self._max_backoff)
        return True


class _AuthenticationStatus(object):
    AUTHENTICATED = 0
    CREDENTIALS_FAILED = 1
    SERIALIZATION_VERSION_MISMATCH = 2
    NOT_ALLOWED_IN_CLUSTER = 3


class ConnectionManager(object):
    """ConnectionManager is responsible for managing ``Connection`` objects."""

    def __init__(
        self,
        client,
        config,
        reactor,
        address_provider,
        lifecycle_service,
        partition_service,
        cluster_service,
        invocation_service,
        near_cache_manager,
    ):
        self.live = False
        self.active_connections = {}  # uuid to connection, must be modified under the _lock
        self.client_uuid = uuid.uuid4()

        self._client = client
        self._config = config
        self._reactor = reactor
        self._address_provider = address_provider
        self._lifecycle_service = lifecycle_service
        self._partition_service = partition_service
        self._cluster_service = cluster_service
        self._invocation_service = invocation_service
        self._near_cache_manager = near_cache_manager
        self._smart_routing_enabled = config.smart_routing
        self._wait_strategy = self._init_wait_strategy(config)
        self._reconnect_mode = config.reconnect_mode
        self._heartbeat_manager = _HeartbeatManager(
            self, self._client, config, reactor, invocation_service
        )
        self._connection_listeners = []
        self._connect_all_members_timer = None
        self._async_start = config.async_start
        self._connect_to_cluster_thread_running = False
        self._shuffle_member_list = config.shuffle_member_list
        self._lock = threading.RLock()
        self._connection_id_generator = AtomicInteger()
        self._labels = frozenset(config.labels)
        self._cluster_id = None
        self._load_balancer = None

    def add_listener(self, on_connection_opened=None, on_connection_closed=None):
        """Registers a ConnectionListener.

        If the same listener is registered multiple times, it will be notified multiple times.

        Args:
            on_connection_opened (function): Function to be called when a connection is opened. (Default value = None)
            on_connection_closed (function): Function to be called when a connection is removed. (Default value = None)
        """
        self._connection_listeners.append((on_connection_opened, on_connection_closed))

    def get_connection(self, member_uuid):
        return self.active_connections.get(member_uuid, None)

    def get_random_connection(self, should_get_data_member=False):
        if self._smart_routing_enabled:
            connection = self._get_connection_from_load_balancer(should_get_data_member)
            if connection:
                return connection

        # We should not get to this point under normal circumstances
        # for the smart client. For uni-socket client, there would be
        # a single connection in the dict. Therefore, copying the list
        # should be acceptable.
        for member_uuid, connection in list(six.iteritems(self.active_connections)):
            if should_get_data_member:
                member = self._cluster_service.get_member(member_uuid)
                if not member or member.lite_member:
                    continue

            return connection

        return None

    def start(self, load_balancer):
        if self.live:
            return

        self.live = True
        self._load_balancer = load_balancer
        self._heartbeat_manager.start()
        self._connect_to_cluster()

    def shutdown(self):
        if not self.live:
            return

        self.live = False
        if self._connect_all_members_timer:
            self._connect_all_members_timer.cancel()

        self._heartbeat_manager.shutdown()

        # Need to create copy of connection values to avoid modification errors on runtime
        for connection in list(six.itervalues(self.active_connections)):
            connection.close("Hazelcast client is shutting down", None)

        self.active_connections.clear()
        del self._connection_listeners[:]

    def connect_to_all_cluster_members(self, sync_start):
        if not self._smart_routing_enabled:
            return

        if sync_start:
            for member in self._cluster_service.get_members():
                try:
                    self._get_or_connect_to_member(member).result()
                except:
                    pass

        self._start_connect_all_members_timer()

    def on_connection_close(self, closed_connection):
        remote_uuid = closed_connection.remote_uuid
        remote_address = closed_connection.remote_address

        if not remote_address:
            _logger.debug(
                "Destroying %s, but it has no remote address, hence nothing is "
                "removed from the connection dictionary",
                closed_connection,
            )
            return

        with self._lock:
            connection = self.active_connections.get(remote_uuid, None)
            disconnected = False
            removed = False
            if connection == closed_connection:
                self.active_connections.pop(remote_uuid, None)
                removed = True
                _logger.info(
                    "Removed connection to %s:%s, connection: %s",
                    remote_address,
                    remote_uuid,
                    connection,
                )

                if not self.active_connections:
                    disconnected = True

        if disconnected:
            self._lifecycle_service.fire_lifecycle_event(LifecycleState.DISCONNECTED)
            self._trigger_cluster_reconnection()

        if removed:
            for _, on_connection_closed in self._connection_listeners:
                if on_connection_closed:
                    try:
                        on_connection_closed(closed_connection)
                    except:
                        _logger.exception("Exception in connection listener")
        else:
            _logger.debug(
                "Destroying %s, but there is no mapping for %s in the connection dictionary",
                closed_connection,
                remote_uuid,
            )

    def check_invocation_allowed(self):
        if self.active_connections:
            return

        if self._async_start or self._reconnect_mode == ReconnectMode.ASYNC:
            raise ClientOfflineError()
        else:
            raise IOError("No connection found to cluster")

    def _get_connection_from_load_balancer(self, should_get_data_member):
        load_balancer = self._load_balancer
        member = None
        if should_get_data_member:
            if load_balancer.can_get_next_data_member():
                member = load_balancer.next_data_member()
        else:
            member = load_balancer.next()

        if not member:
            return None

        return self.get_connection(member.uuid)

    def _get_or_connect_to_address(self, address):
        for connection in list(six.itervalues(self.active_connections)):
            if connection.remote_address == address:
                return ImmediateFuture(connection)

        try:
            translated = self._translate(address)
            connection = self._create_connection(translated)
            return self._authenticate(connection).continue_with(self._on_auth, connection)
        except Exception as e:
            return ImmediateExceptionFuture(e)

    def _get_or_connect_to_member(self, member):
        connection = self.active_connections.get(member.uuid, None)
        if connection:
            return ImmediateFuture(connection)

        try:
            translated = self._translate(member.address)
            connection = self._create_connection(translated)
            return self._authenticate(connection).continue_with(self._on_auth, connection)
        except Exception as e:
            return ImmediateExceptionFuture(e)

    def _create_connection(self, address):
        factory = self._reactor.connection_factory
        return factory(
            self,
            self._connection_id_generator.get_and_increment(),
            address,
            self._config,
            self._invocation_service.handle_client_message,
        )

    def _translate(self, address):
        translated = self._address_provider.translate(address)
        if not translated:
            raise ValueError(
                "Address provider %s could not translate address %s"
                % (self._address_provider.__class__.__name__, address)
            )

        return translated

    def _trigger_cluster_reconnection(self):
        if self._reconnect_mode == ReconnectMode.OFF:
            _logger.info("Reconnect mode is OFF. Shutting down the client")
            self._shutdown_client()
            return

        if self._lifecycle_service.running:
            self._start_connect_to_cluster_thread()

    def _init_wait_strategy(self, config):
        cluster_connect_timeout = config.cluster_connect_timeout
        if cluster_connect_timeout == -1:
            # If the no timeout is specified by the
            # user, or set to -1 explicitly, set
            # the timeout to infinite.
            cluster_connect_timeout = _INF

        return _WaitStrategy(
            config.retry_initial_backoff,
            config.retry_max_backoff,
            config.retry_multiplier,
            cluster_connect_timeout,
            config.retry_jitter,
        )

    def _start_connect_all_members_timer(self):
        connecting_uuids = set()

        def run():
            if not self._lifecycle_service.running:
                return

            for member in self._cluster_service.get_members():
                member_uuid = member.uuid

                if self.active_connections.get(member_uuid, None):
                    continue

                if member_uuid in connecting_uuids:
                    continue

                connecting_uuids.add(member_uuid)
                if not self._lifecycle_service.running:
                    break

                # Bind the bound_member_uuid to the value
                # in this loop iteration
                def cb(_, bound_member_uuid=member_uuid):
                    connecting_uuids.discard(bound_member_uuid)

                self._get_or_connect_to_member(member).add_done_callback(cb)

            self._connect_all_members_timer = self._reactor.add_timer(1, run)

        self._connect_all_members_timer = self._reactor.add_timer(1, run)

    def _connect_to_cluster(self):
        if self._async_start:
            self._start_connect_to_cluster_thread()
        else:
            self._sync_connect_to_cluster()

    def _start_connect_to_cluster_thread(self):
        with self._lock:
            if self._connect_to_cluster_thread_running:
                return

            self._connect_to_cluster_thread_running = True

        def run():
            try:
                while True:
                    self._sync_connect_to_cluster()
                    with self._lock:
                        if self.active_connections:
                            self._connect_to_cluster_thread_running = False
                            return
            except:
                _logger.exception("Could not connect to any cluster, shutting down the client")
                self._shutdown_client()

        t = threading.Thread(target=run, name="hazelcast_async_connection")
        t.daemon = True
        t.start()

    def _shutdown_client(self):
        try:
            self._client.shutdown()
        except:
            _logger.exception("Exception during client shutdown")

    def _sync_connect_to_cluster(self):
        tried_addresses = set()
        self._wait_strategy.reset()
        try:
            while True:
                tried_addresses_per_attempt = set()
                members = self._cluster_service.get_members()
                if self._shuffle_member_list:
                    random.shuffle(members)

                for member in members:
                    self._check_client_active()
                    tried_addresses_per_attempt.add(member.address)
                    connection = self._connect(member, self._get_or_connect_to_member)
                    if connection:
                        return

                for address in self._get_possible_addresses():
                    self._check_client_active()
                    if address in tried_addresses_per_attempt:
                        # We already tried this address on from the member list
                        continue

                    tried_addresses_per_attempt.add(address)
                    connection = self._connect(address, self._get_or_connect_to_address)
                    if connection:
                        return

                tried_addresses.update(tried_addresses_per_attempt)

                # If the address providers load no addresses (which seems to be possible),
                # then the above loop is not entered and the lifecycle check is missing,
                # hence we need to repeat the same check at this point.
                if not tried_addresses_per_attempt:
                    self._check_client_active()

                if not self._wait_strategy.sleep():
                    break
        except (ClientNotAllowedInClusterError, InvalidConfigurationError):
            cluster_name = self._config.cluster_name
            _logger.exception("Stopped trying on cluster %s", cluster_name)

        cluster_name = self._config.cluster_name
        _logger.info(
            "Unable to connect to any address from the cluster with name: %s. "
            "The following addresses were tried: %s",
            cluster_name,
            tried_addresses,
        )
        if self._lifecycle_service.running:
            msg = "Unable to connect to any cluster"
        else:
            msg = "Client is being shutdown"
        raise IllegalStateError(msg)

    def _connect(self, target, get_or_connect_func):
        _logger.info("Trying to connect to %s", target)
        try:
            return get_or_connect_func(target).result()
        except (ClientNotAllowedInClusterError, InvalidConfigurationError) as e:
            _logger.warning("Error during initial connection to %s", target, exc_info=True)
            raise e
        except:
            _logger.warning("Error during initial connection to %s", target, exc_info=True)
            return None

    def _authenticate(self, connection):
        client = self._client
        cluster_name = self._config.cluster_name
        client_name = client.name
        if self._config.token_provider:
            token = self._config.token_provider.token(connection.connected_address)
            request = client_authentication_custom_codec.encode_request(
                cluster_name,
                token,
                self.client_uuid,
                CLIENT_TYPE,
                SERIALIZATION_VERSION,
                __version__,
                client_name,
                self._labels,
            )
        else:
            request = client_authentication_codec.encode_request(
                cluster_name,
                self._config.creds_username,
                self._config.creds_password,
                self.client_uuid,
                CLIENT_TYPE,
                SERIALIZATION_VERSION,
                __version__,
                client_name,
                self._labels,
            )
        invocation = Invocation(
            request, connection=connection, urgent=True, response_handler=lambda m: m
        )
        self._invocation_service.invoke(invocation)
        return invocation.future

    def _on_auth(self, response, connection):
        try:
            response = client_authentication_codec.decode_response(response.result())
        except Exception as err:
            connection.close("Failed to authenticate connection", err)
            raise err

        status = response["status"]
        if status == _AuthenticationStatus.AUTHENTICATED:
            return self._handle_successful_auth(response, connection)

        if status == _AuthenticationStatus.CREDENTIALS_FAILED:
            err = AuthenticationError("Authentication failed. Check cluster name and credentials.")
        elif status == _AuthenticationStatus.NOT_ALLOWED_IN_CLUSTER:
            err = ClientNotAllowedInClusterError("Client is not allowed in the cluster")
        elif status == _AuthenticationStatus.SERIALIZATION_VERSION_MISMATCH:
            err = IllegalStateError("Server serialization version does not match to client")
        else:
            err = AuthenticationError(
                "Authentication status code not supported. status: %s" % status
            )

        connection.close("Failed to authenticate connection", err)
        raise err

    def _handle_successful_auth(self, response, connection):
        with self._lock:
            self._check_partition_count(response["partition_count"])

            server_version_str = response["server_hazelcast_version"]
            remote_address = response["address"]
            remote_uuid = response["member_uuid"]

            connection.remote_address = remote_address
            connection.server_version = calculate_version(server_version_str)
            connection.remote_uuid = remote_uuid

            existing = self.active_connections.get(remote_uuid, None)
            if existing:
                connection.close(
                    "Duplicate connection to same member with UUID: %s" % remote_uuid, None
                )
                return existing

            new_cluster_id = response["cluster_id"]
            changed_cluster = self._cluster_id is not None and self._cluster_id != new_cluster_id
            if changed_cluster:
                self._check_client_state_on_cluster_change(connection)
                _logger.warning(
                    "Switching from current cluster: %s to new cluster: %s",
                    self._cluster_id,
                    new_cluster_id,
                )
                self._on_cluster_restart()

            is_initial_connection = not self.active_connections
            self.active_connections[remote_uuid] = connection
            if is_initial_connection:
                self._cluster_id = new_cluster_id

        if is_initial_connection:
            self._lifecycle_service.fire_lifecycle_event(LifecycleState.CONNECTED)

        _logger.info(
            "Authenticated with server %s:%s, server version: %s, local address: %s",
            remote_address,
            remote_uuid,
            server_version_str,
            connection.local_address,
        )

        for on_connection_opened, _ in self._connection_listeners:
            if on_connection_opened:
                try:
                    on_connection_opened(connection)
                except:
                    _logger.exception("Exception in connection listener")

        if not connection.live:
            self.on_connection_close(connection)

        return connection

    def _check_client_state_on_cluster_change(self, connection):
        if self.active_connections:
            # If there are other connections, we must be connected to the wrong cluster.
            # We should not stay connected to this new connection.
            # Note that, in some racy scenarios, we might close a connection that
            # we can operate on. In those scenarios, we rely on the fact that we will
            # reopen the connections.
            reason = "Connection does not belong to the cluster %s" % self._cluster_id
            connection.close(reason, None)
            raise ValueError(reason)

    def _on_cluster_restart(self):
        self._near_cache_manager.clear_near_caches()
        self._cluster_service.clear_member_list()

    def _check_partition_count(self, partition_count):
        if not self._partition_service.check_and_set_partition_count(partition_count):
            raise ClientNotAllowedInClusterError(
                "Client can not work with this cluster because it has a "
                "different partition count. Expected partition count: %d, "
                "Member partition count: %d"
                % (self._partition_service.partition_count, partition_count)
            )

    def _check_client_active(self):
        if not self._lifecycle_service.running:
            raise HazelcastClientNotActiveError()

    def _get_possible_addresses(self):
        primaries, secondaries = self._address_provider.load_addresses()
        if self._shuffle_member_list:
            # The relative order between primary and secondary addresses should
            # not be changed. So we shuffle the lists separately and then add
            # them to the final list so that secondary addresses are not tried
            # before all primary addresses have been tried. Otherwise we can get
            # startup delays
            random.shuffle(primaries)
            random.shuffle(secondaries)

        addresses = []
        addresses.extend(primaries)
        addresses.extend(secondaries)
        return addresses


class _HeartbeatManager(object):
    _heartbeat_timer = None

    def __init__(self, connection_manager, client, config, reactor, invocation_service):
        self._connection_manager = connection_manager
        self._client = client
        self._reactor = reactor
        self._invocation_service = invocation_service
        self._heartbeat_timeout = config.heartbeat_timeout
        self._heartbeat_interval = config.heartbeat_interval

    def start(self):
        """Starts sending periodic HeartBeat operations."""

        def _heartbeat():
            conn_manager = self._connection_manager
            if not conn_manager.live:
                return

            now = time.time()
            for connection in list(six.itervalues(conn_manager.active_connections)):
                self._check_connection(now, connection)
            self._heartbeat_timer = self._reactor.add_timer(self._heartbeat_interval, _heartbeat)

        self._heartbeat_timer = self._reactor.add_timer(self._heartbeat_interval, _heartbeat)

    def shutdown(self):
        """Stops HeartBeat operations."""
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()

    def _check_connection(self, now, connection):
        if not connection.live:
            return

        if (now - connection.last_read_time) > self._heartbeat_timeout:
            _logger.warning("Heartbeat failed over the connection: %s", connection)
            connection.close(
                "Heartbeat timed out",
                TargetDisconnectedError("Heartbeat timed out to connection %s" % connection),
            )
            return

        if (now - connection.last_write_time) > self._heartbeat_interval:
            request = client_ping_codec.encode_request()
            invocation = Invocation(request, connection=connection, urgent=True)
            self._invocation_service.invoke(invocation)


_frame_header = struct.Struct("<iH")


class _Reader(object):
    def __init__(self, builder):
        self._buf = io.BytesIO()
        self._builder = builder
        self._bytes_read = 0
        self._bytes_written = 0
        # Size of the frame excluding the header (SIZE_OF_FRAME_LENGTH_AND_FLAGS bytes)
        self._frame_size = -1
        self._frame_flags = 0
        self._message = None

    def read(self, data):
        self._buf.seek(self._bytes_written)
        self._buf.write(data)
        self._bytes_written += len(data)

    def process(self):
        message = self._read_message()
        while message:
            self._builder.on_message(message)
            message = self._read_message()

    def _read_message(self):
        while True:
            if self._read_frame():
                if self._message.end_frame.is_final_frame():
                    msg = self._message
                    self._reset()
                    return msg
            else:
                return None

    def _read_frame(self):
        if self._frame_size == -1:
            if self.length < SIZE_OF_FRAME_LENGTH_AND_FLAGS:
                # we don't have even the frame length and flags ready
                return False

            self._read_frame_size_and_flags()

        if self.length < self._frame_size:
            return False

        self._buf.seek(self._bytes_read)
        size = self._frame_size
        data = self._buf.read(size)
        self._bytes_read += size
        self._frame_size = -1
        # No need to reset flags since it will be overwritten on the next read_frame_size_and_flags call
        frame = Frame(data, self._frame_flags)
        if not self._message:
            self._message = InboundMessage(frame)
        else:
            self._message.add_frame(frame)
        return True

    def _read_frame_size_and_flags(self):
        self._buf.seek(self._bytes_read)
        header_data = self._buf.read(SIZE_OF_FRAME_LENGTH_AND_FLAGS)
        self._frame_size, self._frame_flags = _frame_header.unpack_from(header_data, 0)
        self._frame_size -= SIZE_OF_FRAME_LENGTH_AND_FLAGS
        self._bytes_read += SIZE_OF_FRAME_LENGTH_AND_FLAGS

    def _reset(self):
        if self._bytes_written == self._bytes_read:
            self._buf.seek(0)
            self._buf.truncate()
            self._bytes_written = 0
            self._bytes_read = 0
        self._message = None

    @property
    def length(self):
        return self._bytes_written - self._bytes_read


class Connection(object):
    """Connection object which stores connection related information and operations."""

    def __init__(self, connection_manager, connection_id, message_callback):
        self.remote_address = None
        self.remote_uuid = None
        self.connected_address = None
        self.local_address = None
        self.last_read_time = 0
        self.last_write_time = 0
        self.start_time = 0
        self.server_version = UNKNOWN_VERSION
        self.live = True
        self.close_reason = None

        self._connection_manager = connection_manager
        self._id = connection_id
        self._builder = ClientMessageBuilder(message_callback)
        self._reader = _Reader(self._builder)

    def send_message(self, message):
        """Sends a message to this connection.

        Args:
            message (hazelcast.protocol.client_message.OutboundMessage): Message to be sent to this connection.

        Returns:
            bool: ``True`` if the message is written to the socket, ``False`` otherwise.
        """
        if not self.live:
            return False

        self._write(message.buf)
        return True

    def close(self, reason, cause):
        """Closes the connection.

        Args:
            reason (str): The reason this connection is going to be closed. Is allowed to be None.
            cause (Exception): The exception responsible for closing this connection. Is allowed to be None.
        """
        if not self.live:
            return

        self.live = False
        self.close_reason = reason
        self._log_close(reason, cause)
        try:
            self._inner_close()
        except:
            _logger.exception("Error while closing the the connection %s", self)
        self._connection_manager.on_connection_close(self)

    def _log_close(self, reason, cause):
        msg = "%s closed. Reason: %s"
        if reason:
            r = reason
        elif cause:
            r = cause
        else:
            r = "Socket explicitly closed"

        if self._connection_manager.live:
            _logger.info(msg, self, r)
        else:
            _logger.debug(msg, self, r)

    def _inner_close(self):
        raise NotImplementedError()

    def _write(self, buf):
        raise NotImplementedError()

    def __eq__(self, other):
        return isinstance(other, Connection) and self._id == other._id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return self._id


class DefaultAddressProvider(object):
    """Provides initial addresses for client to find and connect to a node.

    It also provides a no-op translator.
    """

    def __init__(self, addresses):
        self._addresses = addresses

    def load_addresses(self):
        """Returns the possible primary and secondary member addresses to connect to."""
        configured_addresses = self._addresses

        if not configured_addresses:
            configured_addresses = ["127.0.0.1"]

        primaries = []
        secondaries = []
        for address in configured_addresses:
            p, s = AddressHelper.get_possible_addresses(address)
            primaries.extend(p)
            secondaries.extend(s)

        return primaries, secondaries

    def translate(self, address):
        """No-op address translator.

        It is there to provide the same API with other address providers.
        """
        return address
