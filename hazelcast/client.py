import logging
import logging.config
import threading

from hazelcast import six
from hazelcast.cluster import ClusterService, _InternalClusterService
from hazelcast.config import _Config
from hazelcast.connection import ConnectionManager, DefaultAddressProvider
from hazelcast.core import DistributedObjectInfo, DistributedObjectEvent
from hazelcast.invocation import InvocationService, Invocation
from hazelcast.listener import ListenerService, ClusterViewListenerService
from hazelcast.lifecycle import LifecycleService, LifecycleState, _InternalLifecycleService
from hazelcast.partition import PartitionService, _InternalPartitionService
from hazelcast.protocol.codec import client_get_distributed_objects_codec, \
    client_add_distributed_object_listener_codec, client_remove_distributed_object_listener_codec
from hazelcast.proxy import ProxyManager, MAP_SERVICE, QUEUE_SERVICE, LIST_SERVICE, SET_SERVICE, MULTI_MAP_SERVICE, \
    REPLICATED_MAP_SERVICE, RINGBUFFER_SERVICE, \
    TOPIC_SERVICE, RELIABLE_TOPIC_SERVICE, \
    EXECUTOR_SERVICE, PN_COUNTER_SERVICE, FLAKE_ID_GENERATOR_SERVICE
from hazelcast.near_cache import NearCacheManager
from hazelcast.reactor import AsyncoreReactor
from hazelcast.serialization import SerializationServiceV1
from hazelcast.statistics import Statistics
from hazelcast.transaction import TWO_PHASE, TransactionManager
from hazelcast.util import AtomicInteger, DEFAULT_LOGGING, RoundRobinLB
from hazelcast.discovery import HazelcastCloudAddressProvider
from hazelcast.errors import IllegalStateError


class HazelcastClient(object):
    """
    Hazelcast Client.
    """
    _CLIENT_ID = AtomicInteger()
    logger = logging.getLogger("HazelcastClient")

    def __init__(self, **kwargs):
        config = _Config.from_dict(kwargs)
        self.config = config
        self._context = _ClientContext()
        client_id = HazelcastClient._CLIENT_ID.get_and_increment()
        self.name = self._create_client_name(client_id)
        self._init_logger()
        self._logger_extras = {"client_name": self.name, "cluster_name": self.config.cluster_name}
        self._reactor = AsyncoreReactor(self._logger_extras)
        self._serialization_service = SerializationServiceV1(config)
        self._near_cache_manager = NearCacheManager(self, self._serialization_service)
        self._internal_lifecycle_service = _InternalLifecycleService(self, self._logger_extras)
        self.lifecycle_service = LifecycleService(self._internal_lifecycle_service)
        self._invocation_service = InvocationService(self, self._reactor, self._logger_extras)
        self._address_provider = self._create_address_provider()
        self._internal_partition_service = _InternalPartitionService(self, self._logger_extras)
        self.partition_service = PartitionService(self._internal_partition_service)
        self._internal_cluster_service = _InternalClusterService(self, self._logger_extras)
        self.cluster_service = ClusterService(self._internal_cluster_service)
        self._connection_manager = ConnectionManager(self, self._reactor, self._address_provider,
                                                     self._internal_lifecycle_service,
                                                     self._internal_partition_service,
                                                     self._internal_cluster_service,
                                                     self._invocation_service,
                                                     self._near_cache_manager,
                                                     self._logger_extras)
        self._load_balancer = self._init_load_balancer(config)
        self._listener_service = ListenerService(self, self._connection_manager,
                                                 self._invocation_service,
                                                 self._logger_extras)
        self._proxy_manager = ProxyManager(self._context)
        self._transaction_manager = TransactionManager(self._context, self._logger_extras)
        self._lock_reference_id_generator = AtomicInteger(1)
        self._statistics = Statistics(self, self._reactor, self._connection_manager,
                                      self._invocation_service, self._near_cache_manager,
                                      self._logger_extras)
        self._cluster_view_listener = ClusterViewListenerService(self, self._connection_manager,
                                                                 self._internal_partition_service,
                                                                 self._internal_cluster_service,
                                                                 self._invocation_service)
        self._shutdown_lock = threading.RLock()
        self._init_context()
        self._start()

    def _init_context(self):
        self._context.init_context(self.config, self._invocation_service, self._internal_partition_service,
                                   self._internal_cluster_service, self._connection_manager,
                                   self._serialization_service, self._listener_service, self._proxy_manager,
                                   self._near_cache_manager, self._lock_reference_id_generator, self._logger_extras)

    def _start(self):
        self._reactor.start()
        try:
            self._internal_lifecycle_service.start()
            self._invocation_service.start(self._internal_partition_service, self._connection_manager,
                                           self._listener_service)
            self._load_balancer.init(self.cluster_service)
            membership_listeners = self.config.membership_listeners
            self._internal_cluster_service.start(self._connection_manager, membership_listeners)
            self._cluster_view_listener.start()
            self._connection_manager.start(self._load_balancer)
            if not self.config.async_start:
                self._internal_cluster_service.wait_initial_member_list_fetched()
                self._connection_manager.connect_to_all_cluster_members()

            self._listener_service.start()
            self._statistics.start()
        except:
            self.shutdown()
            raise
        self.logger.info("Client started.", extra=self._logger_extras)

    def get_executor(self, name):
        """
        Creates cluster-wide :class:`~hazelcast.proxy.executor.Executor`.

        :param name: (str), name of the Executor proxy.
        :return: (:class:`~hazelcast.proxy.executor.Executor`), Executor proxy for the given name.
        """
        return self._proxy_manager.get_or_create(EXECUTOR_SERVICE, name)

    def get_flake_id_generator(self, name):
        """
        Creates or returns a cluster-wide :class:`~hazelcast.proxy.flake_id_generator.FlakeIdGenerator`.

        :param name: (str), name of the FlakeIdGenerator proxy.
        :return: (:class:`~hazelcast.proxy.flake_id_generator.FlakeIdGenerator`), FlakeIdGenerator proxy for the given name
        """
        return self._proxy_manager.get_or_create(FLAKE_ID_GENERATOR_SERVICE, name)

    def get_queue(self, name):
        """
        Returns the distributed queue instance with the specified name.

        :param name: (str), name of the distributed queue.
        :return: (:class:`~hazelcast.proxy.queue.Queue`), distributed queue instance with the specified name.
        """
        return self._proxy_manager.get_or_create(QUEUE_SERVICE, name)

    def get_list(self, name):
        """
        Returns the distributed list instance with the specified name.

        :param name: (str), name of the distributed list.
        :return: (:class:`~hazelcast.proxy.list.List`), distributed list instance with the specified name.
        """
        return self._proxy_manager.get_or_create(LIST_SERVICE, name)

    def get_map(self, name):
        """
        Returns the distributed map instance with the specified name.

        :param name: (str), name of the distributed map.
        :return: (:class:`~hazelcast.proxy.map.Map`), distributed map instance with the specified name.
        """
        return self._proxy_manager.get_or_create(MAP_SERVICE, name)

    def get_multi_map(self, name):
        """
        Returns the distributed MultiMap instance with the specified name.

        :param name: (str), name of the distributed MultiMap.
        :return: (:class:`~hazelcast.proxy.multi_map.MultiMap`), distributed MultiMap instance with the specified name.
        """
        return self._proxy_manager.get_or_create(MULTI_MAP_SERVICE, name)

    def get_pn_counter(self, name):
        """
        Returns the PN Counter instance with the specified name.

        :param name: (str), name of the PN Counter.
        :return: (:class:`~hazelcast.proxy.pn_counter.PNCounter`), the PN Counter.
        """
        return self._proxy_manager.get_or_create(PN_COUNTER_SERVICE, name)

    def get_reliable_topic(self, name):
        """
        Returns the :class:`~hazelcast.proxy.reliable_topic.ReliableTopic` instance with the specified name.

        :param name: (str), name of the ReliableTopic.
        :return: (:class:`~hazelcast.proxy.reliable_topic.ReliableTopic`), the ReliableTopic.
        """
        return self._proxy_manager.get_or_create(RELIABLE_TOPIC_SERVICE, name)

    def get_replicated_map(self, name):
        """
        Returns the distributed ReplicatedMap instance with the specified name.

        :param name: (str), name of the distributed ReplicatedMap.
        :return: (:class:`~hazelcast.proxy.replicated_map.ReplicatedMap`), distributed ReplicatedMap instance with the specified name.
        """
        return self._proxy_manager.get_or_create(REPLICATED_MAP_SERVICE, name)

    def get_ringbuffer(self, name):
        """
        Returns the distributed RingBuffer instance with the specified name.

        :param name: (str), name of the distributed RingBuffer.
        :return: (:class:`~hazelcast.proxy.ringbuffer.RingBuffer`), distributed RingBuffer instance with the specified name.
        """

        return self._proxy_manager.get_or_create(RINGBUFFER_SERVICE, name)

    def get_set(self, name):
        """
        Returns the distributed Set instance with the specified name.

        :param name: (str), name of the distributed Set.
        :return: (:class:`~hazelcast.proxy.set.Set`), distributed Set instance with the specified name.
        """
        return self._proxy_manager.get_or_create(SET_SERVICE, name)

    def get_topic(self, name):
        """
        Returns the :class:`~hazelcast.proxy.topic.Topic` instance with the specified name.

        :param name: (str), name of the Topic.
        :return: (:class:`~hazelcast.proxy.topic.Topic`), the Topic.
        """
        return self._proxy_manager.get_or_create(TOPIC_SERVICE, name)

    def new_transaction(self, timeout=120, durability=1, type=TWO_PHASE):
        """
        Creates a new :class:`~hazelcast.transaction.Transaction` associated with the current thread using default or given options.

        :param timeout: (long), the timeout in seconds determines the maximum lifespan of a transaction. So if a
            transaction is configured with a timeout of 2 minutes, then it will automatically rollback if it hasn't
            committed yet.
        :param durability: (int), the durability is the number of machines that can take over if a member fails during a
        transaction commit or rollback
        :param type: (Transaction Type), the transaction type which can be :const:`~hazelcast.transaction.TWO_PHASE` or :const:`~hazelcast.transaction.ONE_PHASE`
        :return: (:class:`~hazelcast.transaction.Transaction`), new Transaction associated with the current thread.
        """
        return self._transaction_manager.new_transaction(timeout, durability, type)

    def add_distributed_object_listener(self, listener_func):
        """
        Adds a listener which will be notified when a
        new distributed object is created or destroyed.
        :param listener_func: Function to be called when a distributed object is created or destroyed.
        :return: (str), a registration id which is used as a key to remove the listener.
        """
        is_smart = self.config.smart_routing
        request = client_add_distributed_object_listener_codec.encode_request(is_smart)

        def handle_distributed_object_event(name, service_name, event_type, source):
            event = DistributedObjectEvent(name, service_name, event_type, source)
            listener_func(event)

        def event_handler(client_message):
            return client_add_distributed_object_listener_codec.handle(client_message, handle_distributed_object_event)

        def decode_add_listener(response):
            return client_add_distributed_object_listener_codec.decode_response(response)

        def encode_remove_listener(registration_id):
            return client_remove_distributed_object_listener_codec.encode_request(registration_id)

        return self._listener_service.register_listener(request, decode_add_listener,
                                                        encode_remove_listener, event_handler)

    def remove_distributed_object_listener(self, registration_id):
        """
        Removes the specified distributed object listener. Returns silently if there is no such listener added before.
        :param registration_id: (str), id of registered listener.
        :return: (bool), ``true`` if registration is removed, ``false`` otherwise.
        """
        return self._listener_service.deregister_listener(registration_id)

    def get_distributed_objects(self):
        """
        Returns all distributed objects such as; queue, map, set, list, topic, lock, multimap.
        Also, as a side effect, it clears the local instances of the destroyed proxies.
        :return:(Sequence), List of instances created by Hazelcast.
        """
        request = client_get_distributed_objects_codec.encode_request()
        invocation = Invocation(request, response_handler=lambda m: m)
        self._invocation_service.invoke(invocation)
        response = client_get_distributed_objects_codec.decode_response(invocation.future.result())

        distributed_objects = self._proxy_manager.get_distributed_objects()
        local_distributed_object_infos = set()
        for dist_obj in distributed_objects:
            local_distributed_object_infos.add(DistributedObjectInfo(dist_obj.service_name, dist_obj.name))

        for dist_obj_info in response:
            local_distributed_object_infos.discard(dist_obj_info)
            self._proxy_manager.get_or_create(dist_obj_info.service_name, dist_obj_info.name, create_on_remote=False)

        for dist_obj_info in local_distributed_object_infos:
            self._proxy_manager.destroy_proxy(dist_obj_info.service_name, dist_obj_info.name, destroy_on_remote=False)

        return self._proxy_manager.get_distributed_objects()

    def shutdown(self):
        """
        Shuts down this HazelcastClient.
        """
        with self._shutdown_lock:
            if self._internal_lifecycle_service.running:
                self._internal_lifecycle_service.fire_lifecycle_event(LifecycleState.SHUTTING_DOWN)
                self._internal_lifecycle_service.shutdown()
                self._near_cache_manager.destroy_near_caches()
                self._connection_manager.shutdown()
                self._invocation_service.shutdown()
                self._statistics.shutdown()
                self._reactor.shutdown()
                self._internal_lifecycle_service.fire_lifecycle_event(LifecycleState.SHUTDOWN)

    def _create_address_provider(self):
        config = self.config
        cluster_members = config.cluster_members
        address_list_provided = len(cluster_members) > 0
        cloud_discovery_token = config.cloud_discovery_token
        cloud_enabled = cloud_discovery_token is not None
        if address_list_provided and cloud_enabled:
            raise IllegalStateError("Only one discovery method can be enabled at a time. "
                                    "Cluster members given explicitly: %s, Hazelcast Cloud enabled: %s"
                                    % (address_list_provided, cloud_enabled))

        if cloud_enabled:
            connection_timeout = self._get_connection_timeout(config)
            return HazelcastCloudAddressProvider(cloud_discovery_token, connection_timeout, self._logger_extras)

        return DefaultAddressProvider(cluster_members)

    def _init_logger(self):
        config = self.config
        logging_config = config.logging_config
        if logging_config:
            logging.config.dictConfig(logging_config)
        else:
            logging.config.dictConfig(DEFAULT_LOGGING)
            self.logger.setLevel(config.logging_level)

    def _create_client_name(self, client_id):
        client_name = self.config.client_name
        if client_name:
            return client_name
        return "hz.client_%s" % client_id

    @staticmethod
    def _get_connection_timeout(config):
        timeout = config.connection_timeout
        return six.MAXSIZE if timeout == 0 else timeout

    @staticmethod
    def _init_load_balancer(config):
        load_balancer = config.load_balancer
        if not load_balancer:
            load_balancer = RoundRobinLB()
        return load_balancer


class _ClientContext(object):
    """
    Context holding all the required services, managers and the configuration for a Hazelcast client.
    """

    def __init__(self):
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
        self.logger_extras = None

    def init_context(self, config, invocation_service, partition_service,
                     cluster_service, connection_manager, serialization_service,
                     listener_service, proxy_manager, near_cache_manager,
                     lock_reference_id_generator, logger_extras):
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
        self.logger_extras = logger_extras
