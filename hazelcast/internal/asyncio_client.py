import asyncio
import logging
import sys
import typing

from hazelcast.internal.asyncio_cluster import ClusterService, _InternalClusterService
from hazelcast.internal.asyncio_compact import CompactSchemaService
from hazelcast.config import Config, IndexConfig
from hazelcast.internal.asyncio_connection import ConnectionManager, DefaultAsyncioAddressProvider
from hazelcast.core import DistributedObjectEvent, DistributedObjectInfo
from hazelcast.discovery import HazelcastCloudAddressProvider
from hazelcast.errors import IllegalStateError, InvalidConfigurationError
from hazelcast.internal.asyncio_invocation import InvocationService, Invocation
from hazelcast.internal.asyncio_proxy.vector_collection import VectorCollection
from hazelcast.lifecycle import LifecycleService, LifecycleState, _InternalLifecycleService
from hazelcast.internal.asyncio_listener import ClusterViewListenerService, ListenerService
from hazelcast.near_cache import NearCacheManager
from hazelcast.internal.asyncio_partition import PartitionService, InternalPartitionService
from hazelcast.protocol.codec import (
    client_add_distributed_object_listener_codec,
    client_get_distributed_objects_codec,
    client_remove_distributed_object_listener_codec,
    dynamic_config_add_vector_collection_config_codec,
)
from hazelcast.internal.asyncio_proxy.manager import (
    MAP_SERVICE,
    ProxyManager,
    VECTOR_SERVICE,
)
from hazelcast.internal.asyncio_proxy.base import Proxy
from hazelcast.internal.asyncio_proxy.map import Map
from hazelcast.internal.asyncio_reactor import AsyncioReactor
from hazelcast.serialization import SerializationServiceV1
from hazelcast.internal.asyncio_statistics import Statistics
from hazelcast.types import KeyType, ValueType
from hazelcast.util import AtomicInteger, RoundRobinLB

__all__ = ("HazelcastClient",)

_logger = logging.getLogger(__name__)


class HazelcastClient:
    """Hazelcast client instance to access and manipulate distributed data
    structures on the Hazelcast clusters.

    The client can be configured either by:

    - providing a configuration object as the first parameter of the
      constructor

    .. code:: python

        from hazelcast.asyncio import HazelcastClient
        from hazelcast.config import Config

        config = Config()
        config.cluster_name = "a-cluster"
        client = await HazelcastClient.create_and_start(config)

    - passing configuration options as keyword arguments

    .. code:: python

        from hazelcast.asyncio import HazelcastClient

        client = await HazelcastClient.crate_and_start(
            cluster_name="a-cluster",
        )

    Warning:
        Asyncio client is not thread-safe, do not access it from other threads.

    Warning:
        Asyncio client is BETA.
        Its public API may change until General Availability release.

    See the :class:`hazelcast.config.Config` documentation for the possible
    configuration options.
    """

    _CLIENT_ID = AtomicInteger()

    @classmethod
    async def create_and_start(cls, config: Config | None = None, **kwargs) -> "HazelcastClient":
        """Creates a HazelcastClient instance, and starts it.

        .. code:: python

            from hazelcast.asyncio import HazelcastClient

            client = await HazelcastClient.create_and_start()

        See the :class:`hazelcast.config.Config` documentation for the possible
        configuration options.

        Args:
            config: Optional configuration object.
            **kwargs: Optional keyword arguments of the client configuration.

        """

        client = HazelcastClient(config, **kwargs)
        await client._start()
        return client

    def __init__(self, config: Config | None = None, **kwargs):
        """Creates a HazelcastClient instance.

        This call just creates the instance, without starting it.

        The preferred way of creating and starting the client instance is using the ``create_and_start`` method:

        .. code:: python

            from hazelcast.asyncio import HazelcastClient

            client = await HazelcastClient.create_and_start()

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
        self._reactor = AsyncioReactor()
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
        self._internal_partition_service = InternalPartitionService(self)
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
        self._shutdown_lock = asyncio.Lock()
        self._invocation_service.init(
            self._internal_partition_service,
            self._connection_manager,
            self._listener_service,
            self._compact_schema_service,
        )
        self._init_context()

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
            self._reactor,
            self._compact_schema_service,
        )

    async def _start(self):
        try:
            self._internal_lifecycle_service.start()
            await self._invocation_service.start()
            membership_listeners = self._config.membership_listeners
            self._internal_cluster_service.start(self._connection_manager, membership_listeners)
            self._cluster_view_listener.start()
            await self._connection_manager.start(self._load_balancer)
            sync_start = not self._config.async_start
            if sync_start:
                await self._internal_cluster_service.wait_initial_member_list_fetched()
            await self._connection_manager.connect_to_all_cluster_members(sync_start)
            self._listener_service.start()
            await self._invocation_service.add_backup_listener()
            self._load_balancer.init(self._cluster_service)
            await self._statistics.start()
        except Exception:
            await self.shutdown()
            raise
        _logger.info("Client started")

    async def get_map(self, name: str) -> Map[KeyType, ValueType]:
        """Returns the distributed map instance with the specified name.

        Args:
            name: Name of the distributed map.

        Returns:
            Distributed map instance with the specified name.
        """
        return await self._proxy_manager.get_or_create(MAP_SERVICE, name)

    async def create_vector_collection_config(
        self,
        name: str,
        indexes: typing.List[IndexConfig],
        backup_count: int = 1,
        async_backup_count: int = 0,
        split_brain_protection_name: typing.Optional[str] = None,
        merge_policy: str = "PutIfAbsentMergePolicy",
        merge_batch_size: int = 100,
    ) -> None:
        """Creates a vector collection with the given configuration.

        Args:
            name: Name of the distributed map.
            indexes: One or more index configurations. The index names must be unique.
            backup_count: Number of backups to keep for the vector collection.
            split_brain_protection_name: Name of the split brain protection configuration. See https://docs.hazelcast.com/hazelcast/5.6/data-structures/vector-collections#split-brain-protection
            merge_policy: The merge policy to use while recovering in a split brain situation. See https://docs.hazelcast.com/hazelcast/5.6/data-structures/vector-collections#merge-policy
        """
        # check that indexes have different names
        if indexes:
            index_names = set(index.name for index in indexes)
            if len(index_names) != len(indexes):
                raise AssertionError("index names must be unique")

        request = dynamic_config_add_vector_collection_config_codec.encode_request(
            name,
            indexes,
            backup_count,
            async_backup_count,
            split_brain_protection_name,
            merge_policy,
            merge_batch_size,
        )
        invocation = Invocation(request, response_handler=lambda m: m)
        await self._invocation_service.ainvoke(invocation)

    async def get_vector_collection(self, name: str) -> VectorCollection:
        """Returns the vector collection instance with the specified name.

        Args:
            name: Name of the vector collection.

        Returns:
            Vector collection instance with the specified name.
        """
        return await self._proxy_manager.get_or_create(VECTOR_SERVICE, name)

    async def add_distributed_object_listener(
        self, listener_func: typing.Callable[[DistributedObjectEvent], None]
    ) -> str:
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

        return await self._listener_service.register_listener(
            request,
            codec.decode_response,
            client_remove_distributed_object_listener_codec.encode_request,
            event_handler,
        )

    async def remove_distributed_object_listener(self, registration_id: str) -> bool:
        """Removes the specified distributed object listener.

        Returns silently if there is no such listener added before.

        Args:
            registration_id: The id of the registered listener.

        Returns:
            ``True`` if registration is removed, ``False`` otherwise.
        """
        return await self._listener_service.deregister_listener(registration_id)

    async def shutdown(self) -> None:
        """Shuts down this HazelcastClient."""
        async with self._shutdown_lock:
            if self._internal_lifecycle_service.running:
                self._internal_lifecycle_service.fire_lifecycle_event(LifecycleState.SHUTTING_DOWN)
                self._internal_lifecycle_service.shutdown()
                self._near_cache_manager.destroy_near_caches()
                await self._connection_manager.shutdown()
                self._invocation_service.shutdown()
                self._statistics.shutdown()
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

        return DefaultAsyncioAddressProvider(cluster_members)

    def _create_client_name(self, client_id):
        client_name = self._config.client_name
        if client_name:
            return client_name
        return "hz.client_%s" % client_id

    async def _send_state_to_cluster(self):
        return await self._compact_schema_service.send_all_schemas()

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
        self.reactor = reactor
        self.compact_schema_service = compact_schema_service
