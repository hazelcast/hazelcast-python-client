import logging
import sys
import threading
import typing

from hazelcast.cluster import ClusterService, _InternalClusterService
from hazelcast.compact import CompactSchemaService
from hazelcast.config import _Config
from hazelcast.connection import ConnectionManager, DefaultAddressProvider
from hazelcast.core import DistributedObjectEvent, DistributedObjectInfo
from hazelcast.cp import CPSubsystem, ProxySessionManager
from hazelcast.discovery import HazelcastCloudAddressProvider
from hazelcast.errors import IllegalStateError
from hazelcast.future import Future
from hazelcast.invocation import InvocationService, Invocation
from hazelcast.lifecycle import LifecycleService, LifecycleState, _InternalLifecycleService
from hazelcast.listener import ClusterViewListenerService, ListenerService
from hazelcast.near_cache import NearCacheManager
from hazelcast.partition import PartitionService, _InternalPartitionService
from hazelcast.protocol.codec import (
    client_add_distributed_object_listener_codec,
    client_get_distributed_objects_codec,
    client_remove_distributed_object_listener_codec,
)
from hazelcast.proxy import (
    EXECUTOR_SERVICE,
    FLAKE_ID_GENERATOR_SERVICE,
    LIST_SERVICE,
    MAP_SERVICE,
    MULTI_MAP_SERVICE,
    PN_COUNTER_SERVICE,
    QUEUE_SERVICE,
    RELIABLE_TOPIC_SERVICE,
    REPLICATED_MAP_SERVICE,
    RINGBUFFER_SERVICE,
    SET_SERVICE,
    TOPIC_SERVICE,
    Executor,
    FlakeIdGenerator,
    List,
    MultiMap,
    PNCounter,
    ProxyManager,
    Queue,
    ReliableTopic,
    ReplicatedMap,
    Ringbuffer,
    Set,
    Topic,
)
from hazelcast.proxy.base import Proxy
from hazelcast.proxy.map import Map
from hazelcast.reactor import AsyncoreReactor
from hazelcast.serialization import SerializationServiceV1
from hazelcast.sql import SqlService, _InternalSqlService
from hazelcast.statistics import Statistics
from hazelcast.transaction import TWO_PHASE, Transaction, TransactionManager
from hazelcast.types import KeyType, ValueType, ItemType, MessageType
from hazelcast.util import AtomicInteger, RoundRobinLB

__all__ = ("HazelcastClient",)

_logger = logging.getLogger(__name__)


class HazelcastClient:
    """Hazelcast client instance to access and manipulate
    distributed data structures on the Hazelcast clusters.

    Keyword Args:
        cluster_members (`list[str]`): Candidate address list that the client
            will use to establish initial connection. By default, set to
            ``["127.0.0.1"]``.
        cluster_name (str): Name of the cluster to connect to. The name is
            sent as part of the the client authentication message and may
            be verified on the member. By default, set to ``dev``.
        client_name (str): Name of the client instance. By default,
            set to ``hz.client_${CLIENT_ID}``, where ``CLIENT_ID`` starts
            from ``0`` and it is incremented by ``1`` for each new client.
        connection_timeout (float): Socket timeout value in seconds for the
            client to connect member nodes. Setting this to ``0`` makes
            the connection blocking. By default, set to ``5.0``.
        socket_options (`list[tuple]`): List of socket option tuples. The
            tuples must contain the parameters passed into the
            :func:`socket.setsockopt` in the same order.
        redo_operation (bool): When set to ``True``, the client will redo
            the operations that were executing on the server in case if the
            client lost connection. This can happen because of network problems,
            or simply because the member died. However it is not clear whether
            the operation was performed or not. For idempotent operations this is
            harmless, but for non idempotent ones retrying can cause to undesirable
            effects. Note that the redo can be processed on any member. By default,
            set to ``False``.
        smart_routing (bool): Enables smart mode for the client instead of
            unisocket client. Smart clients send key based operations to owner of
            the keys. Unisocket clients send all operations to a single node. By
            default, set to ``True``.
        ssl_enabled (bool): If it is ``True``, SSL is enabled. By default, set
            to ``False``.
        ssl_cafile (str): Absolute path of concatenated CA certificates used to
            validate server's certificates in PEM format. When SSL is enabled and
            cafile is not set, a set of default CA certificates from default
            locations will be used.
        ssl_certfile (str): Absolute path of the client certificate in PEM format.
        ssl_keyfile (str): Absolute path of the private key file for the client
            certificate in the PEM format. If this parameter is ``None``, private
            key will be taken from the certfile.
        ssl_password (str|bytes|bytearray|function): Password for decrypting the
            keyfile if it is encrypted. The password may be a function to call to
            get the password. It will be called with no arguments, and it should
            return a string, bytes, or bytearray. If the return value is a string
            it will be encoded as UTF-8 before using it to decrypt the key.
            Alternatively a string, bytes, or bytearray value may be supplied
            directly as the password.
        ssl_protocol (int|str): Protocol version used in SSL communication.
            By default, set to ``TLSv1_2``. See the
            :class:`hazelcast.config.SSLProtocol` for possible values.
        ssl_ciphers (str): String in the OpenSSL cipher list format to set the
            available ciphers for sockets. More than one cipher can be set by
            separating them with a colon.
        ssl_check_hostname (bool): When set to ``True``, verifies that the
            hostname in the member's certificate and the address of the member
            matches during the handshake. By default, set to ``False``.
        cloud_discovery_token (str): Discovery token of the Hazelcast Cloud cluster.
            When this value is set, Hazelcast Cloud discovery is enabled.
        async_start (bool): Enables non-blocking start mode of the client.
            When set to ``True``, the client creation will not wait to connect to
            cluster. The client instance will throw exceptions until it connects to
            cluster and becomes ready. If set to ``False``, the client
            will block until a cluster connection established and it is ready to use
            the client instance. By default, set to ``False``.
        reconnect_mode (int|str): Defines how the client reconnects to cluster after a
            disconnect. By default, set to ``ON``. See the
            :class:`hazelcast.config.ReconnectMode` for possible values.
        retry_initial_backoff (float): Wait period in seconds after the first failure
            before retrying. Must be non-negative. By default, set to ``1.0``.
        retry_max_backoff (float): Upper bound for the backoff interval in seconds.
            Must be non-negative. By default, set to ``30.0``.
        retry_jitter (float): Defines how much to randomize backoffs. At each iteration
            the calculated back-off is randomized via following method in pseudo-code
            ``Random(-jitter * current_backoff, jitter * current_backoff)``. Must be
            in range ``[0.0, 1.0]``. By default, set to ``0.0`` (no randomization).
        retry_multiplier (float): The factor with which to multiply backoff after a
            failed retry. Must be greater than or equal to ``1``. By default,
            set to ``1.05``.
        cluster_connect_timeout (float): Timeout value in seconds for the client to
            give up connecting to the cluster. Must be non-negative or
            equal to `-1`. By default, set to `-1`. `-1` means that the client
            will not stop trying to the target cluster. (infinite timeout)
        portable_version (int): Default value for the portable version if the
            class does not have the :func:`get_portable_version` method. Portable
            versions are used to differentiate two versions of the
            :class:`hazelcast.serialization.api.Portable` classes that have added
            or removed fields, or fields with different types.
        data_serializable_factories (dict[int, dict[int, class]]): Dictionary of
            factory id and corresponding
            :class:`hazelcast.serialization.api.IdentifiedDataSerializable` factories.
            A factory is simply a dictionary with class id and callable class
            constructors.

            .. code-block:: python

                FACTORY_ID = 1
                CLASS_ID = 1

                class SomeSerializable(IdentifiedDataSerializable):
                    # omitting the implementation
                    pass


                client = HazelcastClient(data_serializable_factories={
                    FACTORY_ID: {
                        CLASS_ID: SomeSerializable
                    }
                })

        portable_factories (dict[int, dict[int, class]]): Dictionary of
            factory id and corresponding
            :class:`hazelcast.serialization.api.Portable` factories.
            A factory is simply a dictionary with class id and callable class
            constructors.

            .. code-block:: python

                FACTORY_ID = 2
                CLASS_ID = 2

                class SomeSerializable(Portable):
                    # omitting the implementation
                    pass


                client = HazelcastClient(portable_factories={
                    FACTORY_ID: {
                        CLASS_ID: SomeSerializable
                    }
                })

        compact_serializers (list[hazelcast.serialization.api.CompactSerializer]):
            List of Compact serializers.

            .. code-block:: python

                class Foo:
                    pass

                class FooSerializer(CompactSerializer[Foo]):
                    pass

                client = HazelcastClient(
                    compact_serializers=[
                        FooSerializer(),
                    ],
                )

        class_definitions (`list[hazelcast.serialization.portable.classdef.ClassDefinition]`):
            List of all portable class definitions.
        check_class_definition_errors (bool): When enabled, serialization system
            will check for class definitions error at start and throw an
            ``HazelcastSerializationError`` with error definition. By default,
            set to ``True``.
        is_big_endian (bool): Defines if big-endian is used as the byte order
            for the serialization. By default, set to ``True``.
        default_int_type (int|str): Defines how the ``int``/``long`` type is
            represented on the cluster side. By default, it is serialized
            as ``INT`` (``32`` bits). See the
            :class:`hazelcast.config.IntType` for possible values.
        global_serializer (hazelcast.serialization.api.StreamSerializer):
            Defines the global serializer. This serializer
            is registered as a fallback serializer to handle all other objects
            if a serializer cannot be located for them.
        custom_serializers (dict[class, hazelcast.serialization.api.StreamSerializer]):
            Dictionary of class and the corresponding custom serializers.

            .. code-block:: python

                class SomeClass:
                    # omitting the implementation
                    pass


                class SomeClassSerializer(StreamSerializer):
                    # omitting the implementation
                    pass

                client = HazelcastClient(custom_serializers={
                    SomeClass: SomeClassSerializer
                })

        near_caches (dict[str, dict[str, any]]): Dictionary of near cache
            names and the corresponding near cache configurations as a dictionary.
            The near cache configurations contains the following options.
            When an option is missing from the configuration, it will be
            set to its default value.

            - **invalidate_on_change** (bool): Enables cluster-assisted invalidate
              on change behavior. When set to ``True``, entries are invalidated
              when they are changed in cluster. By default, set to ``True``.
            - **in_memory_format** (int|str): Specifies in which format data will be
              stored in the Near Cache. By default, set to ``BINARY``. See the
              :class:`hazelcast.config.InMemoryFormat` for possible values.
            - **time_to_live** (float): Maximum number of seconds that an entry can
              stay in cache. When not set, entries won't be evicted due
              to expiration.
            - **max_idle** (float): Maximum number of seconds that an entry can stay
              in the Near Cache until it is accessed. When not set, entries won't be
              evicted due to inactivity.
            - **eviction_policy** (int|str): Defines eviction policy configuration.
              By default, set to ``LRU``. See the
              :class:`hazelcast.config.EvictionPolicy` for possible values.
            - **eviction_max_size** (int): Defines maximum number of entries kept in
              the memory before eviction kicks in. By default, set to ``10000``.
            - **eviction_sampling_count** (int): Number of random entries that are
              evaluated to see if some of them are already expired. By default,
              set to ``8``.
            - **eviction_sampling_pool_size** (int): Size of the pool for eviction
              candidates. The pool is kept sorted according to the eviction policy.
              By default, set to ``16``.

        load_balancer (hazelcast.util.LoadBalancer): Load balancer implementation
            for the client.
        membership_listeners (`list[tuple[function, function]]`): List of membership
            listener tuples. Tuples must be of size ``2``. The first element will
            be the function to be called when a member is added, and the second
            element will be the function to be called when the member is removed
            with the :class:`hazelcast.core.MemberInfo` as the only parameter.
            Any of the elements can be ``None``, but not at the same time.
        lifecycle_listeners (`list[function]`): List of lifecycle listeners.
            Listeners will be called with the new lifecycle state as the only
            parameter when the client changes lifecycle states.
        flake_id_generators (dict[str, dict[str, any]]): Dictionary of flake id
            generator names and the corresponding flake id generator configurations
            as a dictionary. The flake id generator configurations contains the
            following options. When an option is missing from the configuration, it
            will be set to its default value.

            - **prefetch_count** (int): Defines how many IDs are pre-fetched on the
              background when a new flake id is requested from the cluster. Should
              be in the range ``1..100000``. By default, set to ``100``.
            - **prefetch_validity** (float): Defines for how long the pre-fetched IDs
              can be used. If this time elapsed, a new batch of IDs will be fetched.
              Time unit is in seconds. By default, set to ``600`` (10 minutes).

              The IDs contain timestamp component, which ensures rough global ordering
              of IDs. If an ID is assigned to an object that was created much later,
              it will be much out of order. If you don't care about ordering, set this
              value to ``0`` for unlimited ID validity.

        reliable_topics (dict[str, dict[str, any]]): Dictionary of reliable
            topic names and the corresponding reliable topic configurations as
            a dictionary. The reliable topic configurations contain the
            following options. When an option is missing from the
            configuration, it will be set to its default value.

            - **overload_policy** (int|str): Policy to handle an overloaded
              topic. By default, set to ``BLOCK``. See the
              :class:`hazelcast.config.TopicOverloadPolicy` for possible values.
            - **read_batch_size** (int): Number of messages the reliable topic
              will try to read in batch. It will get at least one, but if
              there are more available, then it will try to get more to
              increase throughput. By default, set to ``10``.

        labels (`list[str]`): Labels for the client to be sent to the cluster.
        heartbeat_interval (float): Time interval between the heartbeats sent by the
            client to the member nodes in seconds. By default, set to ``5.0``.
        heartbeat_timeout (float): If there is no message passing between the client
            and a member within the given time via this property in seconds, the
            connection will be closed. By default, set to ``60.0``.
        invocation_timeout (float): When an invocation gets an exception because

            - Member throws an exception.
            - Connection between the client and member is closed.
            - The client's heartbeat requests are timed out.

            Time passed since invocation started is compared with this property.
            If the time is already passed, then the exception is delegated to the user.
            If not, the invocation is retried. Note that, if invocation gets no exception
            and it is a long running one, then it will not get any exception, no matter
            how small this timeout is set. Time unit is in seconds. By default,
            set to ``120.0``.

        invocation_retry_pause (float): Pause time between each retry cycle of an
            invocation in seconds. By default, set to ``1.0``.
        statistics_enabled (bool): When set to ``True``, the client statistics
            collection is enabled. By default, set to ``False``.
        statistics_period (float): The period in seconds the statistics run.
        shuffle_member_list (bool): The Client shuffles the given member list
            to prevent all clients to connect to the same node when this
            property is set to ``True``. When it is set to ``False``, the
            client tries to connect to the nodes in the given order. By
            default, set to ``True``.
        backup_ack_to_client_enabled (bool): Enables the client to get backup
            acknowledgements directly from the member that backups are applied,
            which reduces number of hops and increases performance for smart clients.
            This option has no effect for unisocket clients. By default, set
            to ``True`` (enabled).
        operation_backup_timeout (float): If an operation has backups, defines
            how long the invocation will wait for acks from the backup replicas
            in seconds. If acks are not received from some backups, there won't
            be any rollback on other successful replicas. By default, set to ``5.0``.
        fail_on_indeterminate_operation_state (bool): When enabled, if an operation
            has sync backups and acks are not received from backup replicas in time,
            or the member which owns primary replica of the target partition leaves
            the cluster, then the invocation fails with
            :class:`hazelcast.errors.IndeterminateOperationStateError`. However,
            even if the invocation fails, there will not be any rollback on other
            successful replicas. By default, set to ``False`` (do not fail).
        creds_username (str): Username for credentials authentication (Enterprise feature).
        creds_password (str): Password for credentials authentication (Enterprise feature).
        token_provider (hazelcast.security.TokenProvider): Token provider for
            custom authentication (Enterprise feature). Note that token_provider
            setting has priority over credentials settings.
        use_public_ip (bool): When set to ``True``, the client uses the public
            IP addresses reported by members while connecting to them, if
            available. By default, set to ``False``.
    """

    _CLIENT_ID = AtomicInteger()

    def __init__(self, **kwargs):
        config = _Config.from_dict(kwargs)
        self._config = config
        self._context = _ClientContext()
        client_id = HazelcastClient._CLIENT_ID.get_and_increment()
        self._name = self._create_client_name(client_id)
        self._reactor = AsyncoreReactor()
        self._serialization_service = SerializationServiceV1(config)
        self._near_cache_manager = NearCacheManager(config, self._serialization_service)
        self._internal_lifecycle_service = _InternalLifecycleService(config)
        self._lifecycle_service = LifecycleService(self._internal_lifecycle_service)
        self._invocation_service = InvocationService(self, config, self._reactor)
        self._compact_schema_service = CompactSchemaService(
            self._serialization_service.compact_stream_serializer,
            self._invocation_service,
        )
        self._address_provider = self._create_address_provider()
        self._internal_partition_service = _InternalPartitionService(self)
        self._partition_service = PartitionService(
            self._internal_partition_service,
            self._serialization_service,
            self._compact_schema_service.send_schema_and_retry,
        )
        self._internal_cluster_service = _InternalClusterService(self, config)
        self._cluster_service = ClusterService(self._internal_cluster_service)
        self._connection_manager = ConnectionManager(
            self,
            config,
            self._reactor,
            self._address_provider,
            self._internal_lifecycle_service,
            self._internal_partition_service,
            self._internal_cluster_service,
            self._invocation_service,
            self._near_cache_manager,
            self._send_state_to_cluster,
        )
        self._load_balancer = self._init_load_balancer(config)
        self._listener_service = ListenerService(
            self,
            config,
            self._connection_manager,
            self._invocation_service,
            self._compact_schema_service,
        )
        self._proxy_manager = ProxyManager(self._context)
        self._cp_subsystem = CPSubsystem(self._context)
        self._proxy_session_manager = ProxySessionManager(self._context)
        self._transaction_manager = TransactionManager(self._context)
        self._lock_reference_id_generator = AtomicInteger(1)
        self._statistics = Statistics(
            self,
            config,
            self._reactor,
            self._connection_manager,
            self._invocation_service,
            self._near_cache_manager,
        )
        self._cluster_view_listener = ClusterViewListenerService(
            self,
            self._connection_manager,
            self._internal_partition_service,
            self._internal_cluster_service,
            self._invocation_service,
        )
        self._shutdown_lock = threading.RLock()
        self._invocation_service.init(
            self._internal_partition_service,
            self._connection_manager,
            self._listener_service,
            self._compact_schema_service,
        )
        self._internal_sql_service = _InternalSqlService(
            self._connection_manager,
            self._serialization_service,
            self._invocation_service,
            self._compact_schema_service.send_schema_and_retry,
        )
        self._sql_service = SqlService(self._internal_sql_service)
        self._init_context()
        self._start()

    def _init_context(self):
        self._context.init_context(
            self,
            self._config,
            self._invocation_service,
            self._internal_partition_service,
            self._internal_cluster_service,
            self._connection_manager,
            self._serialization_service,
            self._listener_service,
            self._proxy_manager,
            self._near_cache_manager,
            self._lock_reference_id_generator,
            self._name,
            self._proxy_session_manager,
            self._reactor,
            self._compact_schema_service,
        )

    def _start(self):
        self._reactor.start()
        try:
            self._internal_lifecycle_service.start()
            self._invocation_service.start()
            membership_listeners = self._config.membership_listeners
            self._internal_cluster_service.start(self._connection_manager, membership_listeners)
            self._cluster_view_listener.start()
            self._connection_manager.start(self._load_balancer)
            sync_start = not self._config.async_start
            if sync_start:
                self._internal_cluster_service.wait_initial_member_list_fetched()
            self._connection_manager.connect_to_all_cluster_members(sync_start)

            self._listener_service.start()
            self._invocation_service.add_backup_listener()
            self._load_balancer.init(self._cluster_service)
            self._statistics.start()
        except:
            self.shutdown()
            raise
        _logger.info("Client started")

    def get_executor(self, name: str) -> Executor:
        """Creates cluster-wide ExecutorService.

        Args:
            name: Name of the Executor proxy.

        Returns:
            Executor proxy for the given name.
        """
        return self._proxy_manager.get_or_create(EXECUTOR_SERVICE, name)

    def get_flake_id_generator(self, name: str) -> FlakeIdGenerator:
        """Creates or returns a cluster-wide FlakeIdGenerator.

        Args:
            name: Name of the FlakeIdGenerator proxy.

        Returns:
            FlakeIdGenerator proxy for the given name.
        """
        return self._proxy_manager.get_or_create(FLAKE_ID_GENERATOR_SERVICE, name)

    def get_queue(self, name: str) -> Queue[ItemType]:
        """Returns the distributed queue instance with the specified name.

        Args:
            name: Name of the distributed queue.

        Returns:
            Distributed queue instance with the specified name.
        """
        return self._proxy_manager.get_or_create(QUEUE_SERVICE, name)

    def get_list(self, name: str) -> List[ItemType]:
        """Returns the distributed list instance with the specified name.

        Args:
            name: Name of the distributed list.

        Returns:
            Distributed list instance with the specified name.
        """
        return self._proxy_manager.get_or_create(LIST_SERVICE, name)

    def get_map(self, name: str) -> Map[KeyType, ValueType]:
        """Returns the distributed map instance with the specified name.

        Args:
            name: Name of the distributed map.

        Returns:
            Distributed map instance with the specified name.
        """
        return self._proxy_manager.get_or_create(MAP_SERVICE, name)

    def get_multi_map(self, name: str) -> MultiMap[KeyType, ValueType]:
        """Returns the distributed MultiMap instance with the specified name.

        Args:
            name: Name of the distributed MultiMap.

        Returns:
            Distributed MultiMap instance with the specified name.
        """
        return self._proxy_manager.get_or_create(MULTI_MAP_SERVICE, name)

    def get_pn_counter(self, name: str) -> PNCounter:
        """Returns the PN Counter instance with the specified name.

        Args:
            name: Name of the PN Counter.

        Returns:
            Distributed PN Counter instance with the specified name.
        """
        return self._proxy_manager.get_or_create(PN_COUNTER_SERVICE, name)

    def get_reliable_topic(self, name: str) -> ReliableTopic[MessageType]:
        """Returns the ReliableTopic instance with the specified name.

        Args:
            name: Name of the ReliableTopic.

        Returns:
            Distributed ReliableTopic instance with the specified name.
        """
        return self._proxy_manager.get_or_create(RELIABLE_TOPIC_SERVICE, name)

    def get_replicated_map(self, name: str) -> ReplicatedMap[KeyType, ValueType]:
        """Returns the distributed ReplicatedMap instance with the specified
        name.

        Args:
            name: Name of the distributed ReplicatedMap.

        Returns:
            Distributed ReplicatedMap instance with the specified name.
        """
        return self._proxy_manager.get_or_create(REPLICATED_MAP_SERVICE, name)

    def get_ringbuffer(self, name: str) -> Ringbuffer[ItemType]:
        """Returns the distributed Ringbuffer instance with the specified name.

        Args:
            name: Name of the distributed Ringbuffer.

        Returns:
            Distributed RingBuffer instance with the specified name.
        """

        return self._proxy_manager.get_or_create(RINGBUFFER_SERVICE, name)

    def get_set(self, name: str) -> Set[ItemType]:
        """Returns the distributed Set instance with the specified name.

        Args:
            name: Name of the distributed Set.

        Returns:
            Distributed Set instance with the specified name.
        """
        return self._proxy_manager.get_or_create(SET_SERVICE, name)

    def get_topic(self, name: str) -> Topic[MessageType]:
        """Returns the Topic instance with the specified name.

        Args:
            name: Name of the Topic.

        Returns:
            The Topic.
        """
        return self._proxy_manager.get_or_create(TOPIC_SERVICE, name)

    def new_transaction(
        self, timeout: float = 120, durability: int = 1, type: int = TWO_PHASE
    ) -> Transaction:
        """Creates a new Transaction associated with the current thread
         using default or given options.

        Args:
            timeout: The timeout in seconds determines the maximum lifespan of
                a transaction. So if a transaction is configured with a
                timeout of 2 minutes, then it will automatically rollback if
                it hasn't committed yet.
            durability: The durability is the number of machines that can take
                over if a member fails during a transaction commit or rollback.
            type: The transaction type which can be ``TWO_PHASE``
                or ``ONE_PHASE``.

        Returns:
            New Transaction associated with the current thread.
        """
        return self._transaction_manager.new_transaction(timeout, durability, type)

    def add_distributed_object_listener(
        self, listener_func: typing.Callable[[DistributedObjectEvent], None]
    ) -> Future[str]:
        """Adds a listener which will be notified when a new distributed object
        is created or destroyed.

        Args:
            listener_func: Function to be called when a distributed object is
                created or destroyed.

        Returns:
            A registration id which is used as a key to remove the listener.
        """
        is_smart = self._config.smart_routing
        codec = client_add_distributed_object_listener_codec
        request = codec.encode_request(is_smart)

        def handle_distributed_object_event(name, service_name, event_type, source):
            event = DistributedObjectEvent(name, service_name, event_type, source)
            listener_func(event)

        def event_handler(client_message):
            return codec.handle(client_message, handle_distributed_object_event)

        return self._listener_service.register_listener(
            request,
            codec.decode_response,
            client_remove_distributed_object_listener_codec.encode_request,
            event_handler,
        )

    def remove_distributed_object_listener(self, registration_id: str) -> Future[bool]:
        """Removes the specified distributed object listener.

        Returns silently if there is no such listener added before.

        Args:
            registration_id: The id of registered listener.

        Returns:
            ``True`` if registration is removed, ``False`` otherwise.
        """
        return self._listener_service.deregister_listener(registration_id)

    def get_distributed_objects(self) -> Future[typing.List[Proxy]]:
        """Returns all distributed objects such as; queue, map, set, list,
        topic, lock, multimap.

        Also, as a side effect, it clears the local instances of the destroyed
        proxies.

        Returns:
            List of instances created by Hazelcast.
        """
        request = client_get_distributed_objects_codec.encode_request()
        invocation = Invocation(request, response_handler=lambda m: m)
        self._invocation_service.invoke(invocation)

        local_distributed_object_infos = {
            DistributedObjectInfo(dist_obj.service_name, dist_obj.name)
            for dist_obj in self._proxy_manager.get_distributed_objects()
        }

        response = client_get_distributed_objects_codec.decode_response(invocation.future.result())
        for dist_obj_info in response:
            local_distributed_object_infos.discard(dist_obj_info)
            self._proxy_manager.get_or_create(
                dist_obj_info.service_name, dist_obj_info.name, create_on_remote=False
            )

        for dist_obj_info in local_distributed_object_infos:
            self._proxy_manager.destroy_proxy(
                dist_obj_info.service_name, dist_obj_info.name, destroy_on_remote=False
            )

        return self._proxy_manager.get_distributed_objects()

    def shutdown(self) -> None:
        """Shuts down this HazelcastClient."""
        with self._shutdown_lock:
            if self._internal_lifecycle_service.running:
                self._internal_lifecycle_service.fire_lifecycle_event(LifecycleState.SHUTTING_DOWN)
                self._internal_lifecycle_service.shutdown()
                self._proxy_session_manager.shutdown().result()
                self._near_cache_manager.destroy_near_caches()
                self._connection_manager.shutdown()
                self._invocation_service.shutdown()
                self._statistics.shutdown()
                self._reactor.shutdown()
                self._internal_lifecycle_service.fire_lifecycle_event(LifecycleState.SHUTDOWN)

    @property
    def name(self) -> str:
        """Name of the client."""
        return self._name

    @property
    def lifecycle_service(self) -> LifecycleService:
        """Lifecycle service allows you to check if the client is running and
        add and remove lifecycle listeners.
        """
        return self._lifecycle_service

    @property
    def partition_service(self) -> PartitionService:
        """Partition service allows you to get partition count, introspect
        the partition owners, and partition ids of keys.
        """
        return self._partition_service

    @property
    def cluster_service(self) -> ClusterService:
        """ClusterService: Cluster service allows you to get the list of
        the cluster members and add and remove membership listeners.
        """
        return self._cluster_service

    @property
    def cp_subsystem(self) -> CPSubsystem:
        """CP Subsystem offers set of in-memory linearizable data structures."""
        return self._cp_subsystem

    @property
    def sql(self) -> SqlService:
        """Returns a service to execute distributed SQL queries."""
        return self._sql_service

    def _create_address_provider(self):
        config = self._config
        cluster_members = config.cluster_members
        address_list_provided = len(cluster_members) > 0
        cloud_discovery_token = config.cloud_discovery_token
        cloud_enabled = cloud_discovery_token is not None
        if address_list_provided and cloud_enabled:
            raise IllegalStateError(
                "Only one discovery method can be enabled at a time. "
                "Cluster members given explicitly: %s, Hazelcast Cloud enabled: %s"
                % (address_list_provided, cloud_enabled)
            )

        if cloud_enabled:
            connection_timeout = self._get_connection_timeout(config)
            return HazelcastCloudAddressProvider(cloud_discovery_token, connection_timeout)

        return DefaultAddressProvider(cluster_members)

    def _create_client_name(self, client_id):
        client_name = self._config.client_name
        if client_name:
            return client_name
        return "hz.client_%s" % client_id

    def _send_state_to_cluster(self) -> Future:
        return self._compact_schema_service.send_all_schemas()

    @staticmethod
    def _get_connection_timeout(config):
        timeout = config.connection_timeout
        return sys.maxsize if timeout == 0 else timeout

    @staticmethod
    def _init_load_balancer(config):
        load_balancer = config.load_balancer
        if not load_balancer:
            load_balancer = RoundRobinLB()
        return load_balancer


class _ClientContext:
    """
    Context holding all the required services, managers and the configuration
    for a Hazelcast client.
    """

    def __init__(self):
        self.client = None
        self.config = None
        self.invocation_service = None
        self.partition_service = None
        self.cluster_service = None
        self.connection_manager = None
        self.serialization_service = None
        self.listener_service = None
        self.proxy_manager = None
        self.near_cache_manager = None
        self.lock_reference_id_generator = None
        self.name = None
        self.proxy_session_manager = None
        self.reactor = None
        self.compact_schema_service = None

    def init_context(
        self,
        client,
        config,
        invocation_service,
        partition_service,
        cluster_service,
        connection_manager,
        serialization_service,
        listener_service,
        proxy_manager,
        near_cache_manager,
        lock_reference_id_generator,
        name,
        proxy_session_manager,
        reactor,
        compact_schema_service,
    ):
        self.client = client
        self.config = config
        self.invocation_service = invocation_service
        self.partition_service = partition_service
        self.cluster_service = cluster_service
        self.connection_manager = connection_manager
        self.serialization_service = serialization_service
        self.listener_service = listener_service
        self.proxy_manager = proxy_manager
        self.near_cache_manager = near_cache_manager
        self.lock_reference_id_generator = lock_reference_id_generator
        self.name = name
        self.proxy_session_manager = proxy_session_manager
        self.reactor = reactor
        self.compact_schema_service = compact_schema_service
