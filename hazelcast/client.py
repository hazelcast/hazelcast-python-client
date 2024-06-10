import logging
import sys
import threading
import typing

from hazelcast.cluster import ClusterService, _InternalClusterService
from hazelcast.compact import CompactSchemaService
from hazelcast.config import Config
from hazelcast.connection import ConnectionManager, DefaultAddressProvider
from hazelcast.core import DistributedObjectEvent, DistributedObjectInfo
from hazelcast.cp import CPSubsystem, ProxySessionManager
from hazelcast.discovery import HazelcastCloudAddressProvider
from hazelcast.errors import IllegalStateError, InvalidConfigurationError
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
    dynamic_config_add_vector_collection_config_codec,
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
    VECTOR_SERVICE,
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
from hazelcast.proxy.vector_collection import VectorCollection
from hazelcast.reactor import AsyncoreReactor
from hazelcast.serialization import SerializationServiceV1
from hazelcast.sql import SqlService, _InternalSqlService
from hazelcast.statistics import Statistics
from hazelcast.transaction import TWO_PHASE, Transaction, TransactionManager
from hazelcast.types import KeyType, ValueType, ItemType, MessageType
from hazelcast.util import AtomicInteger, RoundRobinLB

__all__ = ("HazelcastClient",)

from hazelcast.vector import IndexConfig

_logger = logging.getLogger(__name__)


class HazelcastClient:
    """Hazelcast client instance to access and manipulate distributed data
    structures on the Hazelcast clusters.
    """

    _CLIENT_ID = AtomicInteger()

    def __init__(self, config: Config = None, **kwargs):
        """The client can be configured either by:

        - providing a configuration object as the first parameter of the
          constructor

        .. code:: python

            from hazelcast import HazelcastClient
            from hazelcast.config import Config

            config = Config()
            config.cluster_name = "a-cluster"
            client = HazelcastClient(config)

        - passing configuration options as keyword arguments

        .. code:: python

            from hazelcast import HazelcastClient

            client = HazelcastClient(
                cluster_name="a-cluster",
            )


        See the :class:`hazelcast.config.Config` documentation for the possible
        configuration options.

        Args:
            config: Optional configuration object.
            **kwargs: Optional keyword arguments of the client configuration.
        """
        if config:
            if kwargs:
                raise InvalidConfigurationError(
                    "Ambiguous client configuration is found. Either provide "
                    "the config object as the only parameter, or do not "
                    "pass it and use keyword arguments to configure the "
                    "client."
                )
        else:
            config = Config.from_dict(kwargs)

        self._config = config
        self._context = _ClientContext()
        client_id = HazelcastClient._CLIENT_ID.get_and_increment()
        self._name = self._create_client_name(client_id)
        self._reactor = AsyncoreReactor()
        self._serialization_service = SerializationServiceV1(config)
        self._near_cache_manager = NearCacheManager(config, self._serialization_service)
        self._internal_lifecycle_service = _InternalLifecycleService(config)
        self._lifecycle_service = LifecycleService(self._internal_lifecycle_service)
        self._internal_cluster_service = _InternalClusterService(self, config)
        self._cluster_service = ClusterService(self._internal_cluster_service)
        self._invocation_service = InvocationService(self, config, self._reactor)
        self._compact_schema_service = CompactSchemaService(
            self._serialization_service.compact_stream_serializer,
            self._invocation_service,
            self._cluster_service,
            self._reactor,
            self._config,
        )
        self._address_provider = self._create_address_provider()
        self._internal_partition_service = _InternalPartitionService(self)
        self._partition_service = PartitionService(
            self._internal_partition_service,
            self._serialization_service,
            self._compact_schema_service.send_schema_and_retry,
        )
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

    def create_vector_collection_config(self, name: str, indexes: typing.List[IndexConfig]) -> None:
        # check that indexes have different names
        if indexes:
            index_names = set(index.name for index in indexes)
            if len(index_names) != len(indexes):
                raise AssertionError("index names must be unique")

        request = dynamic_config_add_vector_collection_config_codec.encode_request(name, indexes)
        invocation = Invocation(request, response_handler=lambda m: m)
        self._invocation_service.invoke(invocation)
        invocation.future.result()

    def get_vector_collection(self, name: str) -> VectorCollection:
        return self._proxy_manager.get_or_create(VECTOR_SERVICE, name)

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
