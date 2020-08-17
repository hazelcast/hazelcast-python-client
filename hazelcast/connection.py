import logging
import random
import struct
import sys
import threading
import time
import io
import uuid
from collections import OrderedDict

from hazelcast.config import RECONNECT_MODE
from hazelcast.core import AddressHelper, Address
from hazelcast.exception import AuthenticationError, TargetDisconnectedError, HazelcastClientNotActiveError, \
    InvalidConfigurationError, ClientNotAllowedInClusterError, IllegalStateError
from hazelcast.future import ImmediateFuture, ImmediateExceptionFuture
from hazelcast.invocation import Invocation
from hazelcast.lifecycle import LifecycleState
from hazelcast.protocol.client_message import SIZE_OF_FRAME_LENGTH_AND_FLAGS, Frame, InboundMessage, \
    ClientMessageBuilder
from hazelcast.protocol.codec import client_authentication_codec, client_ping_codec
from hazelcast.util import AtomicInteger, calculate_version, UNKNOWN_VERSION, enum
from hazelcast.version import CLIENT_TYPE, CLIENT_VERSION, SERIALIZATION_VERSION
from hazelcast import six

logger = logging.getLogger(__name__)

BUFFER_SIZE = 128000
PROTOCOL_VERSION = 1


class _WaitStrategy(object):
    def __init__(self, initial_backoff, max_backoff, multiplier,
                 cluster_connect_timeout, jitter, logger_extras):
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._multiplier = multiplier
        self._cluster_connect_timeout = cluster_connect_timeout
        self._jitter = jitter
        self._attempt = None
        self._cluster_connect_attempt_begin = None
        self._current_backoff = None
        self._logger_extras = logger_extras

    def reset(self):
        self._attempt = 0
        self._cluster_connect_attempt_begin = time.time()
        self._current_backoff = min(self._max_backoff, self._initial_backoff)

    def sleep(self):
        self._attempt += 1
        now = time.time()
        time_passed = now - self._cluster_connect_attempt_begin
        if time_passed > self._cluster_connect_timeout:
            logger.warning("Unable to get live cluster connection, cluster connect timeout (%d) is reached. "
                           "Attempt %d." % (self._cluster_connect_timeout, self._attempt), extra=self._logger_extras)
            return False

        # random between (-jitter * current_backoff, jitter * current_backoff)
        sleep_time = self._current_backoff + self._current_backoff * self._jitter * (2 * random.random() - 1)
        sleep_time = min(sleep_time, self._cluster_connect_timeout - time_passed)
        logger.warning("Unable to get live cluster connection, retry in %ds, attempt: %d, "
                       "cluster connect timeout: %ds, max backoff: %ds"
                       % (sleep_time, self._attempt, self._cluster_connect_timeout, self._max_backoff),
                       extra=self._logger_extras)
        time.sleep(sleep_time)
        self._current_backoff = min(self._current_backoff * self._multiplier, self._max_backoff)
        return True


_AuthenticationStatus = enum(AUTHENTICATED=0, CREDENTIALS_FAILED=1,
                             SERIALIZATION_VERSION_MISMATCH=2, NOT_ALLOWED_IN_CLUSTER=3)


class ConnectionManager(object):
    """
    ConnectionManager is responsible for managing :mod:`Connection` objects.
    """

    def __init__(self, client, connection_factory, address_provider):
        self.live = False

        self._client = client
        self._connection_factory = connection_factory
        self._heartbeat_manager = _HeartbeatManager(self._client)
        config = self._client.config
        self._smart_routing_enabled = config.network.smart_routing
        self._wait_strategy = self._init_wait_strategy(config)
        self._reconnect_mode = config.connection_strategy.reconnect_mode
        self._connection_listeners = []
        self._connect_all_members_timer = None
        self._async_start = config.connection_strategy.async_start
        self._connect_to_cluster_timer_started = False
        self._connect_to_cluster_timer = None
        self._active_connections = dict()  # access should be synchronized with self._connection_lock
        self._pending_connections = dict()  # access should be synchronized with self._connection_lock
        props = self._client.properties
        self._shuffle_member_list = props.get_bool(props.SHUFFLE_MEMBER_LIST)
        self._address_provider = address_provider
        self._connection_lock = threading.RLock()
        self._connection_id_generator = AtomicInteger()
        self._client_uuid = uuid.uuid4()
        self._labels = config.labels
        self._cluster_id = None
        self._load_balancer = client.load_balancer
        self._logger_extras = {"client_name": self._client.name, "cluster_name": config.cluster_name}

    def add_listener(self, on_connection_opened=None, on_connection_closed=None):
        """
        Registers a ConnectionListener. If the same listener is registered multiple times, it will be notified multiple
        times.

        :param on_connection_opened: (Function), function to be called when a connection is opened.
        :param on_connection_closed: (Function), function to be called when a connection is removed.
        """
        self._connection_listeners.append((on_connection_opened, on_connection_closed))

    def get_connection(self, member_uuid):
        return self._active_connections.get(member_uuid, None)

    def get_connection_from_address(self, address):
        for connection in six.itervalues(self._active_connections):
            if address == connection.remote_address:
                return connection
        return None

    def get_random_connection(self):
        if self._smart_routing_enabled:
            member = self._load_balancer.next()
            if member:
                connection = self.get_connection(member.uuid)
                if connection:
                    return connection

        for connection in six.itervalues(self._active_connections):
            return connection

        return None

    def start(self):
        if self.live:
            return

        self.live = True
        self._heartbeat_manager.start()
        self._connect_to_cluster()
        if self._smart_routing_enabled:
            self._start_connect_all_members_timer()

    def connect_to_all_cluster_members(self):
        if not self._smart_routing_enabled:
            return

        for member in self._client.cluster.get_members():
            try:
                self._get_or_connect(member.address).result()
            except:
                pass

    def on_connection_close(self, connection, cause):
        remote_address = connection.remote_address
        remote_uuid = connection.remote_uuid
        if not remote_address:
            logger.debug("Destroying %s, but it has no remote address, hence nothing is "
                         "removed from the connection dictionary" % connection)
            return

        with self._connection_lock:
            pending = self._pending_connections.pop(remote_address, None)
            connection = self._active_connections.pop(remote_uuid, None)

        if pending:
            pending.set_exception(cause)

        if connection:
            logger.info("Removed connection to %s:%s, connection: %s" % (remote_address, remote_uuid, connection))
            if not self._active_connections:
                self._fire_lifecycle_event(LifecycleState.DISCONNECTED)
                self._trigger_cluster_reconnection()

            for _, on_connection_closed in self._connection_listeners:
                if on_connection_closed:
                    on_connection_closed(connection)
        else:
            logger.debug("Destroying %s, but there is no mapping for %s in the connection dictionary"
                         % (connection, remote_uuid))

    def _trigger_cluster_reconnection(self):
        if self._reconnect_mode == RECONNECT_MODE.OFF:
            logger.info("Reconnect mode is OFF. Shutting down the client")
            self._shutdown_client()
            return

        if self._client.lifecycle.live:
            self._start_connect_to_cluster_timer()


    def _init_wait_strategy(self, config):
        retry_config = config.connection_strategy.retry
        return _WaitStrategy(retry_config.initial_backoff, retry_config.max_backoff, retry_config.multiplier,
                             retry_config.cluster_connect_timeout, retry_config.jitter, self._logger_extras)

    def _start_connect_all_members_timer(self):
        connecting_addresses = set()

        def run():
            lifecycle = self._client.lifecycle
            if not lifecycle.live:
                return

            cluster = self._client.cluster
            for member in cluster.get_members():
                address = member.address

                if not self.get_connection_from_address(address) and address not in connecting_addresses:
                    if not lifecycle.live:
                        continue  # TODO should we break here?

                    if not self.get_connection(member.uuid):
                        self._get_or_connect(address).add_done_callback(lambda: connecting_addresses.remove(address))

            self._connect_all_members_timer = self._client.reactor.add_timer(1, run)

        self._connect_all_members_timer = self._client.reactor.add_timer(1, run)

    def _connect_to_cluster(self):
        if self._async_start:
            # TODO should be executed in another thread
            # self._start_connect_to_cluster_timer()
            pass
        else:
            self._sync_connect_to_cluster()

    def _start_connect_to_cluster_timer(self):
        if self._connect_to_cluster_timer_started:
            return

        def _start():
            try:
                self._sync_connect_to_cluster()
                self._connect_to_cluster_timer_started = False
                if not self._active_connections:
                    reactor = self._client.reactor
                    self._connect_to_cluster_timer = reactor.add_timer(0, self._start_connect_to_cluster_timer)
            except:
                logger.exception("Could not connect to any cluster, shutting down the client")
                self._shutdown_client()

        self._connect_to_cluster_timer = self._client.reactor.add_timer(0, _start)
        self._connect_to_cluster_timer_started = True

    def _shutdown_client(self):
        try:
            self._client.lifecycle.shutdown()
        except:
            logger.exception("Exception during client shutdown")

    def _sync_connect_to_cluster(self):
        tried_addresses = set()
        self._wait_strategy.reset()
        try:
            while True:
                for address in self._get_possible_addresses(self._address_provider):
                    self._check_client_active()
                    tried_addresses.add(address)
                    connection = self._connect(address)
                    if connection:
                        return
                # If the address providers load no addresses (which seems to be possible),
                # then the above loop is not entered and the lifecycle check is missing,
                # hence we need to repeat the same check at this point.
                self._check_client_active()
                if not self._wait_strategy.sleep():
                    break
        except (ClientNotAllowedInClusterError, InvalidConfigurationError):
            cluster_name = self._client.config.cluster_name
            logger.exception("Stopped trying on cluster %s" % cluster_name)

        cluster_name = self._client.config.cluster_name
        logger.info("Unable to connect to any address from the cluster with name: %s. "
                    "The following addresses were tried: %s" % (cluster_name, tried_addresses))
        if self._client.lifecycle.live:
            msg = "Unable to connect to any cluster"
        else:
            msg = "Client is being shutdown"
        raise IllegalStateError(msg)

    def _connect(self, address):
        logger.info("Trying to connect to %s" % address)
        try:
            return self._get_or_connect(address).result()
        except (ClientNotAllowedInClusterError, InvalidConfigurationError) as e:
            logger.exception("Error during initial connection to %s" % address)
            raise e
        except:
            logger.exception("Error during initial connection to %s" % address)
            return None

    def _get_or_connect(self, address):
        with self._connection_lock:
            addr = self.get_connection_from_address(address)
            if addr:
                return ImmediateFuture(addr)
            else:
                pending = self._pending_connections.get(address, None)
                if pending:
                    return pending
                else:
                    try:
                        translated = self._address_provider.translate(address)
                        if not translated:
                            return ImmediateExceptionFuture(
                                ValueError("Address translator could not translate address %s" % address))

                        connection = self._connection_factory(self, self._connection_id_generator.get_and_increment(),
                                                              translated, self._client.config.network,
                                                              self._client.invoker.handle_mesasge)
                    except IOError:
                        return ImmediateExceptionFuture(sys.exc_info()[1], sys.exc_info()[2])

                    future = self._authenticate(connection).continue_with(self._on_auth, connection, address)
                    self._pending_connections[address] = future
                    return future

    def _authenticate(self, connection):
        client = self._client
        cluster_name = client.config.cluster_name
        client_name = client.name
        request = client_authentication_codec.encode_request(cluster_name, None, None, self._client_uuid,
                                                             CLIENT_TYPE, SERIALIZATION_VERSION, CLIENT_VERSION,
                                                             client_name, self._labels)
        return client.invoker.invoke_on_connection(request, connection)

    def _on_auth(self, response, connection, address):
        if response.is_success():
            response = client_authentication_codec.decode_response(response.result())
            status = response["status"]
            if status == _AuthenticationStatus.AUTHENTICATED:
               return self._handle_successful_auth(response, connection, address)

            if status == _AuthenticationStatus.CREDENTIALS_FAILED:
                err = AuthenticationError("Authentication failed. The configured cluster name on "
                                          "the client does not match the one configured in the cluster.")
            elif status == _AuthenticationStatus.NOT_ALLOWED_IN_CLUSTER:
                err = ClientNotAllowedInClusterError("Client is not allowed in the cluster")
            elif status == _AuthenticationStatus.SERIALIZATION_VERSION_MISMATCH:
                err = IllegalStateError("Server serialization version does not match to client")
            else:
                err = AuthenticationError("Authentication status code not supported. status: %s" % status)

            connection.close("Failed to authenticate connection", err)
            raise err
        else:
            e = response.exception()
            connection.close("Failed to authenticate connection", e)
            with self._connection_lock:
                self._pending_connections.pop(address, None)
            six.reraise(e.__class__, e, response.traceback())

    def _handle_successful_auth(self, response, connection, address):
        self._check_partition_count(response["partition_count"])

        server_version_str = response["server_hazelcast_version"]
        remote_address = response["address"]
        remote_uuid = response["member_uuid"]

        connection.remote_address = remote_address
        connection.server_version = calculate_version(server_version_str)
        connection.remote_uuid = remote_uuid

        new_cluster_id = response["cluster_id"]

        is_initial_connection = not self._active_connections
        changed_cluster = is_initial_connection and self._cluster_id and self._cluster_id != new_cluster_id
        if changed_cluster:
            logger.warning("Switching from current cluster: %s to new cluster: %s" % (self._cluster_id, new_cluster_id))
            self._client.on_cluster_restart()

        with self._connection_lock:
            self._active_connections[response["member_uuid"]] = connection
            self._pending_connections.pop(address, None)

        if is_initial_connection:
            self._cluster_id = new_cluster_id
            self._fire_lifecycle_event(LifecycleState.CONNECTED)

        logger.info("Authenticated with server %s:%s, server version: %s, local address: %s"
                    % (remote_address, remote_uuid, server_version_str, connection.local_address))

        for on_connection_opened, _ in self._connection_listeners:
            if on_connection_opened:
                on_connection_opened(connection)  # TODO what if this fails

        if not connection.live:
            self.on_connection_close(connection, None)

        return connection

    def _fire_lifecycle_event(self, state):
        lifecycle = self._client.lifecycle
        lifecycle.fire_lifecycle_event(state)

    def _check_partition_count(self, partition_count):
        partition = self._client.partition
        if not partition.check_and_set_partition_count(partition_count):
            raise ClientNotAllowedInClusterError("Client can not work with this cluster because it has a "
                                                 "different partition count. Expected partition count: %d, "
                                                 "Member partition count: %d"
                                                 % (partition.partition_count, partition_count))

    def _check_client_active(self):
        if not self._client.lifecycle.live:
            raise HazelcastClientNotActiveError()

    def _get_possible_addresses(self, address_provider):
        member_addresses = list(map(lambda m: (m.address, None), self._client.cluster.get_members()))

        if self._shuffle_member_list:
            random.shuffle(member_addresses)

        addresses = OrderedDict(member_addresses)
        primaries, secondaries = address_provider.load_addresses()
        if self._shuffle_member_list:
            random.shuffle(primaries)
            random.shuffle(secondaries)

        for address in primaries:
            addresses[address] = None

        for address in secondaries:
            addresses[address] = None

        return six.iterkeys(addresses)


class _HeartbeatManager(object):
    _heartbeat_timer = None

    def __init__(self, client):
        self._client = client
        self._connection_manager = client.connection_manager
        self._logger_extras = {"client_name": client.name, "cluster_name": client.config.cluster_name}

        props = client.properties
        self._heartbeat_timeout = props.get_seconds_positive_or_default(props.HEARTBEAT_TIMEOUT)
        self._heartbeat_interval = props.get_seconds_positive_or_default(props.HEARTBEAT_INTERVAL)

    def start(self):
        """
        Starts sending periodic HeartBeat operations.
        """

        def _heartbeat():
            if not self._connection_manager.is_alive:
                return

            now = time.time()
            for connection in self._connection_manager.get_active_connections():
                self._check_connection(now, connection)
            self._heartbeat_timer = self._client.reactor.add_timer(self._heartbeat_interval, _heartbeat)

        self._heartbeat_timer = self._client.reactor.add_timer(self._heartbeat_interval, _heartbeat)

    def shutdown(self):
        """
        Stops HeartBeat operations.
        """
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()

    def _check_connection(self, now, connection):
        if not connection.is_alive():
            return

        if (now - connection.last_read_time) > self._heartbeat_timeout:
            if connection.is_alive():
                logger.warning("Heartbeat failed over the connection: %s" % connection)
                connection.close("Heartbeat timed out",
                                 TargetDisconnectedError("Heartbeat timed out to connection %s" % connection))

        if (now - connection.last_write_time) > self._heartbeat_interval:
            request = client_ping_codec.encode_request()
            invoker = self._client.invoker
            invocation = Invocation(invoker, request, connection=connection)
            invoker.invoke_urgent(invocation)


_frame_header = struct.Struct('<iH')


class _Reader(object):
    def __init__(self, builder):
        self._buf = io.BytesIO()
        self._builder = builder
        self._bytes_read = 0
        self._bytes_written = 0
        self._frame_size = 0
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
                if self._message.end_frame.is_end_frame():
                    msg = self._message
                    self._reset()
                    return msg
            else:
                return None

    def _read_frame(self):
        n = self.length
        if n < SIZE_OF_FRAME_LENGTH_AND_FLAGS:
            # we don't have even the frame length and flags ready
            return False

        if self._frame_size == 0:
            self._read_frame_size_and_flags()

        if n < self._frame_size:
            return False

        self._buf.seek(self._bytes_read)
        data = self._buf.read(self._frame_size)
        self._bytes_read += self._frame_size
        self._frame_size = 0
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
        self._frame_size, self._frame_flags = _frame_header.unpack_from(header_data, self._bytes_read)
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
    """
    Connection object which stores connection related information and operations.
    """

    def __init__(self, connection_manager, connection_id, message_callback, logger_extras=None):
        self.remote_address = None
        self.local_address = None
        self.remote_uuid = None
        self.last_read_time = 0
        self.last_write_time = 0
        self.start_time = 0
        self.server_version = UNKNOWN_VERSION
        self.live = True
        self.close_cause = None

        self._connection_manager = connection_manager
        self._logger_extras = logger_extras
        self._id = connection_id
        self._builder = ClientMessageBuilder(message_callback)
        self._reader = _Reader(self._builder)

    def send_message(self, message):
        """
        Sends a message to this connection.

        :param message: (Message), message to be sent to this connection.
        :return: (bool),
        """
        if not self.live:
            return False

        self._write(message.buf)
        return True

    def close(self, reason, cause):
        """
        Closes the connection.

        :param reason: (str), The reason this connection is going to be closed. Is allowed to be None.
        :param cause: (Exception), The exception responsible for closing this connection. Is allowed to be None.
        """
        if not self.live:
            return

        self.live = False
        self._log_close(reason, cause)
        try:
            self._inner_close()
        except:
            logger.exception("Error while closing the the connection %s" % self)
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
            logger.info(msg % (self, r))
        else:
            logger.debug(msg % (self, r))

    def _inner_close(self):
        raise NotImplementedError()

    def _write(self, buf):
        raise NotImplementedError()

    def __repr__(self):
        return "Connection(address=%s, id=%s)" % (self.remote_address, self._id)

    def __eq__(self, other):
        return isinstance(other, Connection) and self._id == other._id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return self._id


class DefaultAddressProvider(object):
    """
    Provides initial addresses for client to find and connect to a node.
    It also provides a no-op translator.
    """

    def __init__(self, addresses):
        self._addresses = addresses

    def load_addresses(self):
        """
        :return: (Tuple), The possible primary and secondary member addresses to connect to.
        """
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
        """
        No-op address translator. It is there to provide the same API
        with other address providers.
        """
        return address
