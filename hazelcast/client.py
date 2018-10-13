import logging
import sys

from hazelcast.cluster import ClusterService, RandomLoadBalancer
from hazelcast.config import ClientConfig, ClientProperties
from hazelcast.connection import ConnectionManager, Heartbeat, DefaultAddressProvider, DefaultAddressTranslator
from hazelcast.invocation import InvocationService, ListenerService
from hazelcast.lifecycle import LifecycleService, LIFECYCLE_STATE_SHUTTING_DOWN, LIFECYCLE_STATE_SHUTDOWN
from hazelcast.partition import PartitionService
from hazelcast.proxy import ProxyManager, MAP_SERVICE, QUEUE_SERVICE, LIST_SERVICE, SET_SERVICE, MULTI_MAP_SERVICE, \
    REPLICATED_MAP_SERVICE, ATOMIC_LONG_SERVICE, ATOMIC_REFERENCE_SERVICE, RINGBUFFER_SERIVCE, COUNT_DOWN_LATCH_SERVICE, \
    TOPIC_SERVICE, RELIABLE_TOPIC_SERVICE, SEMAPHORE_SERVICE, LOCK_SERVICE, ID_GENERATOR_SERVICE, \
    ID_GENERATOR_ATOMIC_LONG_PREFIX, \
    EXECUTOR_SERVICE
from hazelcast.reactor import AsyncoreReactor
from hazelcast.serialization import SerializationServiceV1
from hazelcast.transaction import TWO_PHASE, TransactionManager
from hazelcast.util import LockReferenceIdGenerator
from hazelcast.discovery import HazelcastCloudAddressProvider, HazelcastCloudAddressTranslator, HazelcastCloudDiscovery
from hazelcast.exception import HazelcastIllegalStateError


class HazelcastClient(object):
    """
    Hazelcast Client.
    """
    logger = logging.getLogger("HazelcastClient")
    _config = None

    def __init__(self, config=None):
        self.config = config or ClientConfig()
        self.properties = ClientProperties(self.config.get_properties())
        self.lifecycle = LifecycleService(self.config)
        self.reactor = AsyncoreReactor()
        self._address_providers = self._create_address_providers()
        self._address_translator = self._create_address_translator()
        self.connection_manager = ConnectionManager(self, self.reactor.new_connection, self._address_translator)
        self.heartbeat = Heartbeat(self)
        self.invoker = InvocationService(self)
        self.listener = ListenerService(self)
        self.cluster = ClusterService(self.config, self, self._address_providers)
        self.partition_service = PartitionService(self)
        self.proxy = ProxyManager(self)
        self.load_balancer = RandomLoadBalancer(self.cluster)
        self.serialization_service = SerializationServiceV1(serialization_config=self.config.serialization_config)
        self.transaction_manager = TransactionManager(self)
        self.lock_reference_id_generator = LockReferenceIdGenerator()
        self._start()

    def _start(self):
        self.reactor.start()
        try:
            self.cluster.start()
            self.heartbeat.start()
            self.partition_service.start()
        except:
            self.reactor.shutdown()
            raise
        self.logger.info("Client started.")

    def get_atomic_long(self, name):
        """
        Creates cluster-wide :class:`~hazelcast.proxy.atomic_long.AtomicLong`.

        :param name: (str), name of the AtomicLong proxy.
        :return: (:class:`~hazelcast.proxy.atomic_long.AtomicLong`), AtomicLong proxy for the given name.
        """
        return self.proxy.get_or_create(ATOMIC_LONG_SERVICE, name)

    def get_atomic_reference(self, name):
        """
        Creates cluster-wide :class:`~hazelcast.proxy.atomic_reference.AtomicReference`.

        :param name: (str), name of the AtomicReference proxy.
        :return: (:class:`~hazelcast.proxy.atomic_reference.AtomicReference`), AtomicReference proxy for the given name.
        """
        return self.proxy.get_or_create(ATOMIC_REFERENCE_SERVICE, name)

    def get_count_down_latch(self, name):
        """
        Creates cluster-wide :class:`~hazelcast.proxy.count_down_latch.CountDownLatch`.

        :param name: (str), name of the CountDownLatch proxy.
        :return: (:class:`~hazelcast.proxy.count_down_latch.CountDownLatch`), CountDownLatch proxy for the given name.
        """
        return self.proxy.get_or_create(COUNT_DOWN_LATCH_SERVICE, name)

    def get_executor(self, name):
        """
        Creates cluster-wide :class:`~hazelcast.proxy.executor.Executor`.

        :param name: (str), name of the Executor proxy.
        :return: (:class:`~hazelcast.proxy.executor.Executor`), Executor proxy for the given name.
        """
        return self.proxy.get_or_create(EXECUTOR_SERVICE, name)

    def get_id_generator(self, name):
        """
        Creates cluster-wide :class:`~hazelcast.proxy.id_generator.IdGenerator`.

        :param name: (str), name of the IdGenerator proxy.
        :return: (:class:`~hazelcast.proxy.id_generator.IdGenerator`), IdGenerator proxy for the given name.
        """
        atomic_long = self.get_atomic_long(ID_GENERATOR_ATOMIC_LONG_PREFIX + name)
        return self.proxy.get_or_create(ID_GENERATOR_SERVICE, name, atomic_long=atomic_long)

    def get_queue(self, name):
        """
        Returns the distributed queue instance with the specified name.

        :param name: (str), name of the distributed queue.
        :return: (:class:`~hazelcast.proxy.queue.Queue`), distributed queue instance with the specified name.
        """
        return self.proxy.get_or_create(QUEUE_SERVICE, name)

    def get_list(self, name):
        """
        Returns the distributed list instance with the specified name.

        :param name: (str), name of the distributed list.
        :return: (:class:`~hazelcast.proxy.list.List`), distributed list instance with the specified name.
        """
        return self.proxy.get_or_create(LIST_SERVICE, name)

    def get_lock(self, name):
        """
        Returns the distributed lock instance with the specified name.

        :param name: (str), name of the distributed lock.
        :return: (:class:`~hazelcast.proxy.lock.Lock`), distributed lock instance with the specified name.
        """
        return self.proxy.get_or_create(LOCK_SERVICE, name)

    def get_map(self, name):
        """
        Returns the distributed map instance with the specified name.

        :param name: (str), name of the distributed map.
        :return: (:class:`~hazelcast.proxy.map.Map`), distributed map instance with the specified name.
        """
        return self.proxy.get_or_create(MAP_SERVICE, name)

    def get_multi_map(self, name):
        """
        Returns the distributed MultiMap instance with the specified name.

        :param name: (str), name of the distributed MultiMap.
        :return: (:class:`~hazelcast.proxy.multi_map.MultiMap`), distributed MultiMap instance with the specified name.
        """
        return self.proxy.get_or_create(MULTI_MAP_SERVICE, name)

    def get_reliable_topic(self, name):
        """
        Returns the :class:`~hazelcast.proxy.reliable_topic.ReliableTopic` instance with the specified name.

        :param name: (str), name of the ReliableTopic.
        :return: (:class:`~hazelcast.proxy.reliable_topic.ReliableTopic`), the ReliableTopic.
        """
        return self.proxy.get_or_create(RELIABLE_TOPIC_SERVICE, name)

    def get_replicated_map(self, name):
        """
        Returns the distributed ReplicatedMap instance with the specified name.

        :param name: (str), name of the distributed ReplicatedMap.
        :return: (:class:`~hazelcast.proxy.replicated_map.ReplicatedMap`), distributed ReplicatedMap instance with the specified name.
        """
        return self.proxy.get_or_create(REPLICATED_MAP_SERVICE, name)

    def get_ringbuffer(self, name):
        """
        Returns the distributed RingBuffer instance with the specified name.

        :param name: (str), name of the distributed RingBuffer.
        :return: (:class:`~hazelcast.proxy.ringbuffer.RingBuffer`), distributed RingBuffer instance with the specified name.
        """

        return self.proxy.get_or_create(RINGBUFFER_SERIVCE, name)

    def get_semaphore(self, name):
        """
        Returns the distributed Semaphore instance with the specified name.

        :param name: (str), name of the distributed Semaphore.
        :return: (:class:`~hazelcast.proxy.semaphore.Semaphore`), distributed Semaphore instance with the specified name.
        """
        return self.proxy.get_or_create(SEMAPHORE_SERVICE, name)

    def get_set(self, name):
        """
        Returns the distributed Set instance with the specified name.

        :param name: (str), name of the distributed Set.
        :return: (:class:`~hazelcast.proxy.set.Set`), distributed Set instance with the specified name.
        """
        return self.proxy.get_or_create(SET_SERVICE, name)

    def get_topic(self, name):
        """
        Returns the :class:`~hazelcast.proxy.topic.Topic` instance with the specified name.

        :param name: (str), name of the Topic.
        :return: (:class:`~hazelcast.proxy.topic.Topic`), the Topic.
        """
        return self.proxy.get_or_create(TOPIC_SERVICE, name)

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
        return self.transaction_manager.new_transaction(timeout, durability, type)

    def shutdown(self):
        """
        Shuts down this HazelcastClient.
        """
        if self.lifecycle.is_live:
            self.lifecycle.fire_lifecycle_event(LIFECYCLE_STATE_SHUTTING_DOWN)
            self.partition_service.shutdown()
            self.heartbeat.shutdown()
            self.cluster.shutdown()
            self.reactor.shutdown()
            self.lifecycle.fire_lifecycle_event(LIFECYCLE_STATE_SHUTDOWN)
            self.logger.info("Client shutdown.")

    def _create_address_providers(self):
        network_config = self.config.network_config
        address_providers = []

        cloud_config = network_config.cloud_config
        cloud_address_provider = self._init_cloud_address_provider(cloud_config)
        if cloud_address_provider:
            address_providers.append(cloud_address_provider)

        address_providers.append(DefaultAddressProvider(network_config))
        return address_providers

    def _init_cloud_address_provider(self, cloud_config):
        if cloud_config.enabled:
            discovery_token = cloud_config.discovery_token
            host, url = HazelcastCloudDiscovery.get_host_and_url(self.config.get_properties(), discovery_token)
            return HazelcastCloudAddressProvider(host, url, self._get_connection_timeout())

        cloud_token = self.properties.get(self.properties.HAZELCAST_CLOUD_DISCOVERY_TOKEN)
        if cloud_token != "":
            host, url = HazelcastCloudDiscovery.get_host_and_url(self.config.get_properties(), cloud_token)
            return HazelcastCloudAddressProvider(host, url, self._get_connection_timeout())

        return None

    def _create_address_translator(self):
        network_config = self.config.network_config
        cloud_config = network_config.cloud_config
        cloud_discovery_token = self.properties.get(self.properties.HAZELCAST_CLOUD_DISCOVERY_TOKEN)

        address_list_provided = len(network_config.addresses) != 0
        if cloud_discovery_token != "" and cloud_config.enabled:
            raise HazelcastIllegalStateError("Ambiguous Hazelcast.cloud configuration. "
                                             "Both property based and client configuration based settings are provided "
                                             "for Hazelcast cloud discovery together. Use only one.")

        hazelcast_cloud_enabled = cloud_discovery_token != "" or cloud_config.enabled
        self._is_discovery_configuration_consistent(address_list_provided, hazelcast_cloud_enabled)

        if hazelcast_cloud_enabled:
            if cloud_config.enabled:
                discovery_token = cloud_config.discovery_token
            else:
                discovery_token = cloud_discovery_token
            host, url = HazelcastCloudDiscovery.get_host_and_url(self.config.get_properties(), discovery_token)
            return HazelcastCloudAddressTranslator(host, url, self._get_connection_timeout())

        return DefaultAddressTranslator()

    def _get_connection_timeout(self):
        network_config = self.config.network_config
        conn_timeout = network_config.connection_timeout
        return sys.maxsize if conn_timeout == 0 else conn_timeout

    def _is_discovery_configuration_consistent(self, address_list_provided, hazelcast_cloud_enabled):
        count = 0
        if address_list_provided:
            count += 1
        if hazelcast_cloud_enabled:
            count += 1

        if count > 1:
            raise HazelcastIllegalStateError("Only one discovery method can be enabled at a time. "
                                             "Cluster members given explicitly: {}"
                                             ", Hazelcast.cloud enabled: {}".format(address_list_provided,
                                                                                    hazelcast_cloud_enabled))
