import logging
import threading

from hazelcast import six
from hazelcast.cluster import ClusterService, _InternalClusterService
from hazelcast.config import _Config
from hazelcast.connection import ConnectionManager, DefaultAddressProvider
from hazelcast.core import DistributedObjectInfo, DistributedObjectEvent
from hazelcast.cp import CPSubsystem, ProxySessionManager
from hazelcast.invocation import InvocationService, Invocation
from hazelcast.listener import ListenerService, ClusterViewListenerService
from hazelcast.lifecycle import LifecycleService, LifecycleState, _InternalLifecycleService
from hazelcast.partition import PartitionService, _InternalPartitionService
from hazelcast.protocol.codec import (
    client_get_distributed_objects_codec,
    client_add_distributed_object_listener_codec,
    client_remove_distributed_object_listener_codec,
)
from hazelcast.proxy import (
    ProxyManager,
    MAP_SERVICE,
    QUEUE_SERVICE,
    LIST_SERVICE,
    SET_SERVICE,
    MULTI_MAP_SERVICE,
    REPLICATED_MAP_SERVICE,
    RINGBUFFER_SERVICE,
    TOPIC_SERVICE,
    RELIABLE_TOPIC_SERVICE,
    EXECUTOR_SERVICE,
    PN_COUNTER_SERVICE,
    FLAKE_ID_GENERATOR_SERVICE,
)
from hazelcast.near_cache import NearCacheManager
from hazelcast.reactor import AsyncoreReactor
from hazelcast.serialization import SerializationServiceV1
from hazelcast.statistics import Statistics
from hazelcast.transaction import TWO_PHASE, TransactionManager
from hazelcast.util import AtomicInteger, RoundRobinLB
from hazelcast.discovery import HazelcastCloudAddressProvider
from hazelcast.errors import IllegalStateError

_logger = logging.getLogger(__name__)


class HazelcastClient(object):
    """Hazelcast client instance to access access and manipulate
    distributed data structures on the Hazelcast clusters.

    Keyword Args:
        cluster_members (`list[str]`): Candidate address list that client
            will use to establish initial connection.
            By default, set to ``["127.0.0.1"]``.
        cluster_name (str): Name of the cluster to connect to. The name is
            sent as part of the the client authentication message and may
            be verified on the member. By default, set to ``dev``.
        client_name (str): Name of the client instance. By default,
            set to ``hz.client_${CLIENT_ID}``, where ``CLIENT_ID`` starts
            from ``0`` and it is incremented by ``1`` for each new client.
        connection_timeout (float): Socket timeout value in seconds for
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
        cloud_discovery_token (str): Discovery token of the Hazelcast Cloud cluster.
            When this value is set, Hazelcast Cloud discovery is enabled.
        async_start (bool): Enables non-blocking start mode of :class:`HazelcastClient`.
            When set to ``True``, the client creation will not wait to connect to
            cluster. The client instance will throw exceptions until it connects to
            cluster and becomes ready. If set to ``False``, :class:`HazelcastClient`
            will block until a cluster connection established and it is ready to use
            the client instance. By default, set to ``False``.
        reconnect_mode (int|str): Defines how a client reconnects to cluster after a
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
            set to ``1.0``.
        cluster_connect_timeout (float): Timeout value in seconds for the client to
            give up a connection attempt to the cluster. Must be non-negative.
            By default, set to `120.0`.
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

                class SomeClass(object):
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
            for the client
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

        labels (`list[str]`): Labels for the client to be sent to the cluster.
        heartbeat_interval (float): Time interval between the heartbeats sent by the
            client to the member nodes in seconds. By default, set to ``5.0``.
        heartbeat_timeout (float): If there is no message passing between the client
            and a member within the given time via this property in seconds, the
            connection will be closed. By default, set to ``60.0``.
        invocation_timeout (float): When an invocation gets an exception because

            - Member throws an exception.
            - Connection between the client and member is closed.
            - Client's heartbeat requests are timed out.

            Time passed since invocation started is compared with this property.
            If the time is already passed, then the exception is delegated to the user.
            If not, the invocation is retried. Note that, if invocation gets no exception
            and it is a long running one, then it will not get any exception, no matter
            how small this timeout is set. Time unit is in seconds. By default,
            set to ``120.0``.

        invocation_retry_pause (float): Pause time between each retry cycle of an
            invocation in seconds. By default, set to ``1.0``.
        statistics_enabled (bool): When set to ``True``, client statistics collection
            is enabled. By default, set to ``False``.
        statistics_period (float): The period in seconds the statistics run.
        shuffle_member_list (bool): Client shuffles the given member list to prevent
            all clients to connect to the same node when this property is set to
            ``True``. When it is set to ``False``, the client tries to connect to
            the nodes in the given order. By default, set to ``True``.
        backup_ack_to_client_enabled (bool): Enables client to get backup
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
    """

    _CLIENT_ID = AtomicInteger()

    def __init__(self, **kwargs):
        config = _Config.from_dict(kwargs)
        self._config = config
        self._context = _ClientContext()
        client_id = HazelcastClient._CLIENT_ID.get_and_increment()
        self.name = self._create_client_name(client_id)
        self._reactor = AsyncoreReactor()
        self._serialization_service = SerializationServiceV1(config)
        self._near_cache_manager = NearCacheManager(config, self._serialization_service)
        self._internal_lifecycle_service = _InternalLifecycleService(config)
        self.lifecycle_service = LifecycleService(self._internal_lifecycle_service)
        self._invocation_service = InvocationService(self, config, self._reactor)
        self._address_provider = self._create_address_provider()
        self._internal_partition_service = _InternalPartitionService(self)
        self.partition_service = PartitionService(
            self._internal_partition_service, self._serialization_service
        )
        self._internal_cluster_service = _InternalClusterService(self, config)
        self.cluster_service = ClusterService(self._internal_cluster_service)
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
        )
        self._load_balancer = self._init_load_balancer(config)
        self._listener_service = ListenerService(
            self, config, self._connection_manager, self._invocation_service
        )
        self._proxy_manager = ProxyManager(self._context)
        self.cp_subsystem = CPSubsystem(self._context)
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
        self._init_context()
        self._start()

    def _init_context(self):
        self._context.init_context(
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
            self.name,
            self._proxy_session_manager,
            self._reactor,
        )

    def _start(self):
        self._reactor.start()
        try:
            self._invocation_service.init(
                self._internal_partition_service, self._connection_manager, self._listener_service
            )
            self._internal_lifecycle_service.start()
            membership_listeners = self._config.membership_listeners
            self._internal_cluster_service.start(self._connection_manager, membership_listeners)
            self._cluster_view_listener.start()
            self._connection_manager.start(self._load_balancer)
            if not self._config.async_start:
                self._internal_cluster_service.wait_initial_member_list_fetched()
                self._connection_manager.connect_to_all_cluster_members()

            self._listener_service.start()
            self._invocation_service.start()
            self._load_balancer.init(self.cluster_service)
            self._statistics.start()
        except:
            self.shutdown()
            raise
        _logger.info("Client started")

    def get_executor(self, name):
        """Creates cluster-wide ExecutorService.

        Args:
            name (str): Name of the Executor proxy.

        Returns:
            hazelcast.proxy.executor.Executor: Executor proxy for the given name.
        """
        return self._proxy_manager.get_or_create(EXECUTOR_SERVICE, name)

    def get_flake_id_generator(self, name):
        """Creates or returns a cluster-wide FlakeIdGenerator.

        Args:
            name (str): Name of the FlakeIdGenerator proxy.

        Returns:
            hazelcast.proxy.flake_id_generator.FlakeIdGenerator: FlakeIdGenerator proxy for the given name

        """
        return self._proxy_manager.get_or_create(FLAKE_ID_GENERATOR_SERVICE, name)

    def get_queue(self, name):
        """Returns the distributed queue instance with the specified name.

        Args:
            name (str): Name of the distributed queue.

        Returns:
            hazelcast.proxy.queue.Queue: Distributed queue instance with the specified name.
        """
        return self._proxy_manager.get_or_create(QUEUE_SERVICE, name)

    def get_list(self, name):
        """Returns the distributed list instance with the specified name.

        Args:
            name (str): Name of the distributed list.

        Returns:
            hazelcast.proxy.list.List: Distributed list instance with the specified name.
        """
        return self._proxy_manager.get_or_create(LIST_SERVICE, name)

    def get_map(self, name):
        """Returns the distributed map instance with the specified name.

        Args:
            name (str): Name of the distributed map.

        Returns:
            hazelcast.proxy.map.Map: Distributed map instance with the specified name.
        """
        return self._proxy_manager.get_or_create(MAP_SERVICE, name)

    def get_multi_map(self, name):
        """Returns the distributed MultiMap instance with the specified name.

        Args:
            name (str): Name of the distributed MultiMap.

        Returns:
            hazelcast.proxy.multi_map.MultiMap: Distributed MultiMap instance with the specified name.
        """
        return self._proxy_manager.get_or_create(MULTI_MAP_SERVICE, name)

    def get_pn_counter(self, name):
        """Returns the PN Counter instance with the specified name.

        Args:
            name (str): Name of the PN Counter.

        Returns:
            hazelcast.proxy.pn_counter.PNCounter: The PN Counter.
        """
        return self._proxy_manager.get_or_create(PN_COUNTER_SERVICE, name)

    def get_reliable_topic(self, name):
        """Returns the ReliableTopic instance with the specified name.

        Args:
            name (str): Name of the ReliableTopic.

        Returns:
            hazelcast.proxy.reliable_topic.ReliableTopic: The ReliableTopic.
        """
        return self._proxy_manager.get_or_create(RELIABLE_TOPIC_SERVICE, name)

    def get_replicated_map(self, name):
        """Returns the distributed ReplicatedMap instance with the specified name.

        Args:
            name (str): Name of the distributed ReplicatedMap.

        Returns:
            hazelcast.proxy.replicated_map.ReplicatedMap: Distributed ReplicatedMap instance with the specified name.
        """
        return self._proxy_manager.get_or_create(REPLICATED_MAP_SERVICE, name)

    def get_ringbuffer(self, name):
        """Returns the distributed Ringbuffer instance with the specified name.

        Args:
            name (str): Name of the distributed Ringbuffer.

        Returns:
            hazelcast.proxy.ringbuffer.Ringbuffer: Distributed RingBuffer instance with the specified name.
        """

        return self._proxy_manager.get_or_create(RINGBUFFER_SERVICE, name)

    def get_set(self, name):
        """Returns the distributed Set instance with the specified name.

        Args:
            name (str): Name of the distributed Set.

        Returns:
            hazelcast.proxy.set.Set: Distributed Set instance with the specified name.
        """
        return self._proxy_manager.get_or_create(SET_SERVICE, name)

    def get_topic(self, name):
        """Returns the Topic instance with the specified name.

        Args:
            name (str): Name of the Topic.

        Returns:
            hazelcast.proxy.topic.Topic: The Topic.
        """
        return self._proxy_manager.get_or_create(TOPIC_SERVICE, name)

    def new_transaction(self, timeout=120, durability=1, type=TWO_PHASE):
        """Creates a new Transaction associated with the current thread using default or given options.

        Args:
            timeout (int): The timeout in seconds determines the maximum lifespan of a transaction. So if a
                transaction is configured with a timeout of 2 minutes, then it will automatically rollback if it hasn't
                committed yet.
            durability (int): The durability is the number of machines that can take over if a member fails during a
                transaction commit or rollback.
            type (int): The transaction type which can be ``TWO_PHASE`` or ``ONE_PHASE``.

        Returns:
            hazelcast.transaction.Transaction: New Transaction associated with the current thread.
        """
        return self._transaction_manager.new_transaction(timeout, durability, type)

    def add_distributed_object_listener(self, listener_func):
        """Adds a listener which will be notified when a new distributed object is created or destroyed.

        Args:
            listener_func (function): Function to be called when a distributed object is created or destroyed.

        Returns:
            hazelcast.future.Future[str]: A registration id which is used as a key to remove the listener.
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

    def remove_distributed_object_listener(self, registration_id):
        """Removes the specified distributed object listener.

        Returns silently if there is no such listener added before.

        Args:
            registration_id (str): The id of registered listener.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if registration is removed, ``False`` otherwise.
        """
        return self._listener_service.deregister_listener(registration_id)

    def get_distributed_objects(self):
        """Returns all distributed objects such as; queue, map, set, list, topic, lock, multimap.

        Also, as a side effect, it clears the local instances of the destroyed proxies.

        Returns:
            list[hazelcast.proxy.base.Proxy]: List of instances created by Hazelcast.
        """
        request = client_get_distributed_objects_codec.encode_request()
        invocation = Invocation(request, response_handler=lambda m: m)
        self._invocation_service.invoke(invocation)
        response = client_get_distributed_objects_codec.decode_response(invocation.future.result())

        distributed_objects = self._proxy_manager.get_distributed_objects()
        local_distributed_object_infos = set()
        for dist_obj in distributed_objects:
            local_distributed_object_infos.add(
                DistributedObjectInfo(dist_obj.service_name, dist_obj.name)
            )

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

    def shutdown(self):
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
        self.name = None
        self.proxy_session_manager = None
        self.reactor = None

    def init_context(
        self,
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
    ):
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
