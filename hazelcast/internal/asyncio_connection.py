import asyncio
import io
import logging
import random
import struct
import time
import uuid
from typing import Coroutine

from hazelcast import __version__
from hazelcast.config import ReconnectMode
from hazelcast.core import (
    AddressHelper,
    CLIENT_TYPE,
    SERIALIZATION_VERSION,
    EndpointQualifier,
    ProtocolType,
)
from hazelcast.errors import (
    AuthenticationError,
    TargetDisconnectedError,
    HazelcastClientNotActiveError,
    InvalidConfigurationError,
    ClientNotAllowedInClusterError,
    IllegalStateError,
    ClientOfflineError,
)
from hazelcast.internal.asyncio_invocation import Invocation
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
from hazelcast.util import (
    AtomicInteger,
    calculate_version,
    UNKNOWN_VERSION,
    member_of_larger_same_version_group,
)

_logger = logging.getLogger(__name__)

_INF = float("inf")
_SQL_CONNECTION_RANDOM_ATTEMPTS = 10
_CLIENT_PUBLIC_ENDPOINT_QUALIFIER = EndpointQualifier(ProtocolType.CLIENT, "public")


class WaitStrategy:
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


class AuthenticationStatus:
    AUTHENTICATED = 0
    CREDENTIALS_FAILED = 1
    SERIALIZATION_VERSION_MISMATCH = 2
    NOT_ALLOWED_IN_CLUSTER = 3


class ClientState:
    INITIAL = 0
    """
    Clients start with this state. 
    Once a client connects to a cluster, it directly switches to 
    `INITIALIZED_ON_CLUSTER` instead of `CONNECTED_TO_CLUSTER` because on 
    startup a client has no local state to send to the cluster.
    """

    CONNECTED_TO_CLUSTER = 1
    """
    When a client switches to a new cluster, it moves to this state. It means 
    that the client has connected to a new cluster but not sent its local 
    state to the new cluster yet.
    """

    INITIALIZED_ON_CLUSTER = 2
    """
    When a client sends its local state to the cluster it has connected, it 
    switches to this state.
    Invocations are allowed in this state.
    """


class ConnectionManager:
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
        send_state_to_cluster_fn,
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
        self._send_state_to_cluster_fn = send_state_to_cluster_fn
        self._client_state = ClientState.INITIAL  # must be modified under the _lock
        self._established_initial_cluster_connection = False  # must be modified under the _lock
        self._smart_routing_enabled = config.smart_routing
        self._wait_strategy = self._init_wait_strategy(config)
        self._reconnect_mode = config.reconnect_mode
        self._heartbeat_manager = HeartbeatManager(
            self, self._client, config, reactor, invocation_service
        )
        self._connection_listeners = []
        self._connect_all_members_timer = None
        self._async_start = config.async_start
        self._connect_to_cluster_thread_running = False
        self._shuffle_member_list = config.shuffle_member_list
        self._lock = asyncio.Lock()
        self._connection_id_generator = AtomicInteger()
        self._labels = frozenset(config.labels)
        self._cluster_id = None
        self._load_balancer = None
        self._use_public_ip = (
            isinstance(address_provider, DefaultAddressProvider) and config.use_public_ip
        )

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

    def get_random_connection(self):
        # Try getting the connection from the load balancer, if smart routing is enabled
        if self._smart_routing_enabled:
            member = self._load_balancer.next()
            if member:
                connection = self.get_connection(member.uuid)
                if connection:
                    return connection

        # Otherwise iterate over connections and return the first one
        for connection in list(self.active_connections.values()):
            return connection

        # Failed to get a connection
        return None

    def get_random_connection_for_sql(self):
        """Returns a random connection for SQL.

        The connection is tried to be selected in the following order.

            - Random connection to a data member from the larger same-version
              group.
            - Random connection to a data member.
            - Any random connection
            - ``None``, if there is no connection.

        Returns:
            Connection: A random connection for SQL.
        """
        if self._smart_routing_enabled:
            # There might be a race - the chosen member might be just connected or disconnected.
            # Try a couple of times, the member_of_larger_same_version_group returns a random
            # connection, we might be lucky...
            for _ in range(_SQL_CONNECTION_RANDOM_ATTEMPTS):
                members = self._cluster_service.get_members()
                member = member_of_larger_same_version_group(members)
                if not member:
                    break

                connection = self.get_connection(member.uuid)
                if connection:
                    return connection

        # Otherwise iterate over connections and return the first one
        # that's not to a lite member.
        first_connection = None
        for member_uuid, connection in list(self.active_connections.items()):
            if not first_connection:
                first_connection = connection

            member = self._cluster_service.get_member(member_uuid)
            if not member or member.lite_member:
                continue

            return connection

        # Failed to get a connection to a data member.
        return first_connection

    async def start(self, load_balancer):
        if self.live:
            return

        self.live = True
        self._load_balancer = load_balancer
        self._heartbeat_manager.start()
        await self._connect_to_cluster()

    async def shutdown(self):
        if not self.live:
            return

        self.live = False
        if self._connect_all_members_timer:
            self._connect_all_members_timer.cancel()

        self._heartbeat_manager.shutdown()

        # Need to create copy of connection values to avoid modification errors on runtime
        async with asyncio.TaskGroup() as tg:
            for connection in list(self.active_connections.values()):
                tg.create_task(
                    connection.close_connection("Hazelcast client is shutting down", None)
                )

        self.active_connections.clear()
        del self._connection_listeners[:]

    async def connect_to_all_cluster_members(self, sync_start):
        if not self._smart_routing_enabled:
            return

        if sync_start:
            async with asyncio.TaskGroup() as tg:
                for member in self._cluster_service.get_members():
                    tg.create_task(self._get_or_connect_to_member(member))

        self._start_connect_all_members_timer()

    async def on_connection_close(self, closed_connection):
        remote_uuid = closed_connection.remote_uuid
        remote_address = closed_connection.remote_address

        if not remote_address:
            _logger.debug(
                "Destroying %s, but it has no remote address, hence nothing is "
                "removed from the connection dictionary",
                closed_connection,
            )
            return

        disconnected = False
        removed = False
        trigger_reconnection = False
        async with self._lock:
            connection = self.active_connections.get(remote_uuid, None)
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
                    trigger_reconnection = True
                    if self._client_state == ClientState.INITIALIZED_ON_CLUSTER:
                        disconnected = True

        if disconnected:
            self._lifecycle_service.fire_lifecycle_event(LifecycleState.DISCONNECTED)

        if trigger_reconnection:
            await self._trigger_cluster_reconnection()

        if removed:
            async with asyncio.TaskGroup() as tg:
                # TODO: see on_connection_open
                for _, on_connection_closed in self._connection_listeners:
                    if on_connection_closed:
                        try:
                            maybe_coro = on_connection_closed(closed_connection)
                            if isinstance(maybe_coro, Coroutine):
                                tg.create_task(maybe_coro)
                        except Exception:
                            _logger.exception("Exception in connection listener")
        else:
            _logger.debug(
                "Destroying %s, but there is no mapping for %s in the connection dictionary",
                closed_connection,
                remote_uuid,
            )

    def check_invocation_allowed(self):
        state = self._client_state
        if state == ClientState.INITIALIZED_ON_CLUSTER and self.active_connections:
            return

        if state == ClientState.INITIAL:
            if self._async_start:
                raise ClientOfflineError()
            else:
                raise IOError("No connection found to cluster since the client is starting.")
        elif self._reconnect_mode == ReconnectMode.ASYNC:
            raise ClientOfflineError()
        else:
            raise IOError("No connection found to cluster")

    def initialized_on_cluster(self) -> bool:
        """
        Returns ``True`` if the client is initialized on the cluster, by
        sending its local state, if necessary.
        """
        return self._client_state == ClientState.INITIALIZED_ON_CLUSTER

    async def _get_or_connect_to_address(self, address):
        for connection in list(self.active_connections.values()):
            if connection.remote_address == address:
                return connection
        translated = self._translate(address)
        connection = await self._create_connection(translated)
        response = await self._authenticate(connection)
        await self._on_auth(response, connection)
        return connection

    async def _get_or_connect_to_member(self, member):
        connection = self.active_connections.get(member.uuid, None)
        if connection:
            return connection

        translated = self._translate_member_address(member)
        connection = await self._create_connection(translated)
        response = await self._authenticate(connection)  # .continue_with(self._on_auth, connection)
        await self._on_auth(response, connection)
        return connection

    async def _create_connection(self, address):
        factory = self._reactor.connection_factory
        return await factory(
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

    def _translate_member_address(self, member):
        if self._use_public_ip:
            public_address = member.address_map.get(_CLIENT_PUBLIC_ENDPOINT_QUALIFIER, None)
            if public_address:
                return public_address

            return member.address

        return self._translate(member.address)

    async def _trigger_cluster_reconnection(self):
        if self._reconnect_mode == ReconnectMode.OFF:
            _logger.info("Reconnect mode is OFF. Shutting down the client")
            await self._shutdown_client()
            return

        if self._lifecycle_service.running:
            await self._start_connect_to_cluster_thread()

    def _init_wait_strategy(self, config):
        cluster_connect_timeout = config.cluster_connect_timeout
        if cluster_connect_timeout == -1:
            # If the no timeout is specified by the
            # user, or set to -1 explicitly, set
            # the timeout to infinite.
            cluster_connect_timeout = _INF

        return WaitStrategy(
            config.retry_initial_backoff,
            config.retry_max_backoff,
            config.retry_multiplier,
            cluster_connect_timeout,
            config.retry_jitter,
        )

    def _start_connect_all_members_timer(self):
        connecting_uuids = set()

        async def run():
            if not self._lifecycle_service.running:
                return

            async with asyncio.TaskGroup() as tg:
                member_uuids = []
                for member in self._cluster_service.get_members():
                    member_uuid = member.uuid
                    if self.active_connections.get(member_uuid, None):
                        continue
                    if member_uuid in connecting_uuids:
                        continue
                    connecting_uuids.add(member_uuid)
                    if not self._lifecycle_service.running:
                        break
                    # TODO: ERROR:asyncio:Task was destroyed but it is pending!
                    tg.create_task(self._get_or_connect_to_member(member))
                    member_uuids.append(member_uuid)

            for item in member_uuids:
                connecting_uuids.discard(item)

            self._connect_all_members_timer = self._reactor.add_timer(
                1, lambda: asyncio.create_task(run())
            )

        self._connect_all_members_timer = self._reactor.add_timer(
            1, lambda: asyncio.create_task(run())
        )

    async def _connect_to_cluster(self):
        await self._sync_connect_to_cluster()

    async def _start_connect_to_cluster_thread(self):
        async with self._lock:
            if self._connect_to_cluster_thread_running:
                return

            self._connect_to_cluster_thread_running = True

        try:
            while True:
                await self._sync_connect_to_cluster()
                async with self._lock:
                    if self.active_connections:
                        self._connect_to_cluster_thread_running = False
                        return
        except Exception:
            _logger.exception("Could not connect to any cluster, shutting down the client")
            await self._shutdown_client()

    async def _shutdown_client(self):
        try:
            await self._client.shutdown()
        except Exception:
            _logger.exception("Exception during client shutdown")

    async def _sync_connect_to_cluster(self):
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
                    connection = await self._connect(member, self._get_or_connect_to_member)
                    if connection:
                        return

                for address in self._get_possible_addresses():
                    self._check_client_active()
                    if address in tried_addresses_per_attempt:
                        # We already tried this address on from the member list
                        continue

                    tried_addresses_per_attempt.add(address)
                    connection = await self._connect(address, self._get_or_connect_to_address)
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

    async def _connect(self, target, get_or_connect_func):
        _logger.info("Trying to connect to %s", target)
        try:
            return await get_or_connect_func(target)
        except (ClientNotAllowedInClusterError, InvalidConfigurationError) as e:
            _logger.warning("Error during initial connection to %s", target, exc_info=True)
            raise e
        except Exception:
            _logger.warning("Error during initial connection to %s", target, exc_info=True)
            return None

    def _authenticate(self, connection) -> asyncio.Future:
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

    async def _on_auth(self, response, connection):
        try:
            response = client_authentication_codec.decode_response(response)
        except Exception as e:
            await connection.close_connection("Failed to authenticate connection", e)
            raise e

        status = response["status"]
        if status == AuthenticationStatus.AUTHENTICATED:
            return await self._handle_successful_auth(response, connection)

        if status == AuthenticationStatus.CREDENTIALS_FAILED:
            err = AuthenticationError("Authentication failed. Check cluster name and credentials.")
        elif status == AuthenticationStatus.NOT_ALLOWED_IN_CLUSTER:
            err = ClientNotAllowedInClusterError("Client is not allowed in the cluster")
        elif status == AuthenticationStatus.SERIALIZATION_VERSION_MISMATCH:
            err = IllegalStateError("Server serialization version does not match to client")
        else:
            err = AuthenticationError(
                "Authentication status code not supported. status: %s" % status
            )

        await connection.close_connection("Failed to authenticate connection", err)
        raise err

    async def _handle_successful_auth(self, response, connection):
        async with self._lock:
            self._check_partition_count(response["partition_count"])

            server_version_str = response["server_hazelcast_version"]
            remote_address = response["address"]
            remote_uuid = response["member_uuid"]

            connection.remote_address = remote_address
            connection.server_version = calculate_version(server_version_str)
            connection.remote_uuid = remote_uuid

            existing = self.active_connections.get(remote_uuid, None)
            if existing:
                await connection.close_connection(
                    "Duplicate connection to same member with UUID: %s" % remote_uuid, None
                )
                return existing

            new_cluster_id = response["cluster_id"]
            changed_cluster = self._cluster_id is not None and self._cluster_id != new_cluster_id
            if changed_cluster:
                await self._check_client_state_on_cluster_change(connection)
                _logger.warning(
                    "Switching from current cluster: %s to new cluster: %s",
                    self._cluster_id,
                    new_cluster_id,
                )
                self._on_cluster_restart()

            is_initial_connection = not self.active_connections
            self.active_connections[remote_uuid] = connection
            fire_connected_lifecycle_event = False
            if is_initial_connection:
                self._cluster_id = new_cluster_id
                # In split brain, the client might connect to the one half
                # of the cluster, and then later might reconnect to the
                # other half, after the half it was connected to is
                # completely dead. Since the cluster id is preserved in
                # split brain scenarios, it is impossible to distinguish
                # reconnection to the same cluster vs reconnection to the
                # other half of the split brain. However, in the latter,
                # we might need to send some state to the other half of
                # the split brain (like Compact schemas). That forces us
                # to send the client state to the cluster after the first
                # cluster connection, regardless the cluster id is
                # changed or not.
                if self._established_initial_cluster_connection:
                    self._client_state = ClientState.CONNECTED_TO_CLUSTER
                    await self._initialize_on_cluster(new_cluster_id)
                else:
                    fire_connected_lifecycle_event = True
                    self._established_initial_cluster_connection = True
                    self._client_state = ClientState.INITIALIZED_ON_CLUSTER

        if fire_connected_lifecycle_event:
            self._lifecycle_service.fire_lifecycle_event(LifecycleState.CONNECTED)

        _logger.info(
            "Authenticated with server %s:%s, server version: %s, local address: %s",
            remote_address,
            remote_uuid,
            server_version_str,
            connection.local_address,
        )

        async with asyncio.TaskGroup() as tg:
            for on_connection_opened, _ in self._connection_listeners:
                if on_connection_opened:
                    try:
                        # TODO: creating the task may not throw the exception
                        # TODO: protect the loop against exceptions, so all handlers run
                        maybe_coro = on_connection_opened(connection)
                        if isinstance(maybe_coro, Coroutine):
                            tg.create_task(maybe_coro)
                    except Exception:
                        _logger.exception("Exception in connection listener")

        if not connection.live:
            await self.on_connection_close(connection)

        return connection

    async def _initialize_on_cluster(self, cluster_id) -> None:
        # This method is only called in the reactor thread
        if cluster_id != self._cluster_id:
            _logger.warning(
                f"Client won't send the state to the cluster: {cluster_id}"
                f"because it switched to a new cluster: {self._cluster_id}"
            )
            return

        async def callback():
            try:
                if cluster_id == self._cluster_id:
                    _logger.debug("The client state is sent to the cluster %s", cluster_id)
                    self._client_state = ClientState.INITIALIZED_ON_CLUSTER
                    self._lifecycle_service.fire_lifecycle_event(LifecycleState.CONNECTED)
                elif _logger.isEnabledFor(logging.DEBUG):
                    _logger.warning(
                        "Cannot set client state to 'INITIALIZED_ON_CLUSTER'"
                        f"because current cluster id: {self._cluster_id}"
                        f"is different than the expected cluster id: {cluster_id}"
                    )
            except Exception:
                await retry_on_error()

        async def retry_on_error():
            _logger.exception(f"Failure during sending client state to the cluster {cluster_id}")

            if cluster_id != self._cluster_id:
                return

            if _logger.isEnabledFor(logging.DEBUG):
                _logger.warning(f"Retrying sending client state to the cluster: {cluster_id}")

            await self._initialize_on_cluster(cluster_id)

        try:
            await self._send_state_to_cluster_fn()
            await callback()
        except Exception:
            await retry_on_error()

    async def _check_client_state_on_cluster_change(self, connection):
        if self.active_connections:
            # If there are other connections, we must be connected to the wrong cluster.
            # We should not stay connected to this new connection.
            # Note that, in some racy scenarios, we might close a connection that
            # we can operate on. In those scenarios, we rely on the fact that we will
            # reopen the connections.
            reason = "Connection does not belong to the cluster %s" % self._cluster_id
            await connection.close_connection(reason, None)
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


class HeartbeatManager:
    def __init__(self, connection_manager, client, config, reactor, invocation_service):
        self._connection_manager = connection_manager
        self._client = client
        self._reactor = reactor
        self._invocation_service = invocation_service
        self._heartbeat_timeout = config.heartbeat_timeout
        self._heartbeat_interval = config.heartbeat_interval
        self._heartbeat_timer = None

    def start(self):
        """Starts sending periodic HeartBeat operations."""

        async def _heartbeat():
            conn_manager = self._connection_manager
            if not conn_manager.live:
                return

            now = time.time()
            async with asyncio.TaskGroup() as tg:
                for connection in list(conn_manager.active_connections.values()):
                    tg.create_task(self._check_connection(now, connection))
            self._heartbeat_timer = self._reactor.add_timer(
                self._heartbeat_interval, lambda: asyncio.create_task(_heartbeat())
            )

        self._heartbeat_timer = self._reactor.add_timer(
            self._heartbeat_interval, lambda: asyncio.create_task(_heartbeat())
        )

    def shutdown(self):
        """Stops HeartBeat operations."""
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()

    async def _check_connection(self, now, connection):
        if not connection.live:
            return

        if (now - connection.last_read_time) > self._heartbeat_timeout:
            _logger.warning("Heartbeat failed over the connection: %s", connection)
            await connection.close_connection(
                "Heartbeat timed out",
                TargetDisconnectedError("Heartbeat timed out to connection %s" % connection),
            )
            return

        if (now - connection.last_write_time) > self._heartbeat_interval:
            request = client_ping_codec.encode_request()
            invocation = Invocation(request, connection=connection, urgent=True)
            self._invocation_service.invoke(invocation)


_frame_header = struct.Struct("<iH")


class _Reader:
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


class Connection:
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

    # Not named close to distinguish it from the asyncore.dispatcher.close.
    async def close_connection(self, reason, cause):
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
        except Exception:
            _logger.exception("Error while closing the the connection %s", self)
        await self._connection_manager.on_connection_close(self)

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


class DefaultAddressProvider:
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
