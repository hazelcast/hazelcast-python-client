import asyncio
import logging
import sys
import typing

from hazelcast.internal.asyncio_cluster import ClusterService, _InternalClusterService
from hazelcast.internal.asyncio_compact import CompactSchemaService
from hazelcast.config import Config
from hazelcast.internal.asyncio_connection import ConnectionManager, DefaultAddressProvider
from hazelcast.core import DistributedObjectEvent, DistributedObjectInfo
from hazelcast.cp import CPSubsystem, ProxySessionManager
from hazelcast.discovery import HazelcastCloudAddressProvider
from hazelcast.errors import IllegalStateError, InvalidConfigurationError
from hazelcast.internal.asyncio_invocation import InvocationService, Invocation
from hazelcast.lifecycle import LifecycleService, LifecycleState, _InternalLifecycleService
from hazelcast.internal.asyncio_listener import ClusterViewListenerService, ListenerService
from hazelcast.near_cache import NearCacheManager
from hazelcast.partition import PartitionService, _InternalPartitionService
from hazelcast.protocol.codec import (
    client_add_distributed_object_listener_codec,
    client_get_distributed_objects_codec,
    client_remove_distributed_object_listener_codec,
)
from hazelcast.internal.asyncio_proxy.manager import (
    MAP_SERVICE,
    ProxyManager,
)
from hazelcast.internal.asyncio_proxy.base import Proxy
from hazelcast.internal.asyncio_proxy.map import Map
from hazelcast.internal.asyncio_reactor import AsyncioReactor
from hazelcast.serialization import SerializationServiceV1
from hazelcast.sql import SqlService, _InternalSqlService
from hazelcast.statistics import Statistics
from hazelcast.types import KeyType, ValueType, ItemType, MessageType
from hazelcast.util import AtomicInteger, RoundRobinLB

__all__ = ("HazelcastClient",)

_logger = logging.getLogger(__name__)


class HazelcastClient:

    _CLIENT_ID = AtomicInteger()

    @classmethod
    async def create_and_start(cls, config: Config|None = None, **kwargs) -> "HazelcastClient":
        client = HazelcastClient(config, **kwargs)
        await client._start()
        return client

    def __init__(self, config: Config|None = None, **kwargs):
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
        self._internal_sql_service = _InternalSqlService(
            self._connection_manager,
            self._serialization_service,
            self._invocation_service,
            self._compact_schema_service.send_schema_and_retry,
        )
        self._sql_service = SqlService(self._internal_sql_service)
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
            self._proxy_session_manager,
            self._reactor,
            self._compact_schema_service,
        )

    async def _start(self):
        self._reactor.start()
        try:
            self._internal_lifecycle_service.start()
            self._invocation_service.start()
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
            self._statistics.start()
        except Exception:
            await self.shutdown()
            raise
        _logger.info("Client started")

    async def get_map(self, name: str) -> Map[KeyType, ValueType]:
        return await self._proxy_manager.get_or_create(MAP_SERVICE, name)

    async def add_distributed_object_listener(
        self, listener_func: typing.Callable[[DistributedObjectEvent], None]
    ) -> str:
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
        return await self._listener_service.deregister_listener(registration_id)

    async def get_distributed_objects(self) -> typing.List[Proxy]:
        request = client_get_distributed_objects_codec.encode_request()
        invocation = Invocation(request, response_handler=lambda m: m)
        await self._invocation_service.ainvoke(invocation)

        local_distributed_object_infos = {
            DistributedObjectInfo(dist_obj.service_name, dist_obj.name)
            for dist_obj in self._proxy_manager.get_distributed_objects()
        }

        response = client_get_distributed_objects_codec.decode_response(invocation.future.result())
        async with asyncio.TaskGroup() as tg:  # type: ignore[attr-defined]
            for dist_obj_info in response:
                local_distributed_object_infos.discard(dist_obj_info)
                tg.create_task(
                    self._proxy_manager.get_or_create(
                        dist_obj_info.service_name, dist_obj_info.name, create_on_remote=False
                    )
                )

        async with asyncio.TaskGroup() as tg:  # type: ignore[attr-defined]
            for dist_obj_info in local_distributed_object_infos:
                tg.create_task(
                    self._proxy_manager.destroy_proxy(
                        dist_obj_info.service_name, dist_obj_info.name, destroy_on_remote=False
                    )
                )

        return self._proxy_manager.get_distributed_objects()

    async def shutdown(self) -> None:
        async with self._shutdown_lock:
            if self._internal_lifecycle_service.running:
                self._internal_lifecycle_service.fire_lifecycle_event(LifecycleState.SHUTTING_DOWN)
                self._internal_lifecycle_service.shutdown()
                self._proxy_session_manager.shutdown().result()
                self._near_cache_manager.destroy_near_caches()
                await self._connection_manager.shutdown()
                self._invocation_service.shutdown()
                self._statistics.shutdown()
                self._reactor.shutdown()
                self._internal_lifecycle_service.fire_lifecycle_event(LifecycleState.SHUTDOWN)

    @property
    def name(self) -> str:
        return self._name

    @property
    def lifecycle_service(self) -> LifecycleService:
        return self._lifecycle_service

    @property
    def partition_service(self) -> PartitionService:
        return self._partition_service

    @property
    def cluster_service(self) -> ClusterService:
        return self._cluster_service

    @property
    def cp_subsystem(self) -> CPSubsystem:
        return self._cp_subsystem

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
