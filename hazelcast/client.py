import logging
from hazelcast.cluster import ClusterService, RandomLoadBalancer
from hazelcast.config import ClientConfig
from hazelcast.connection import ConnectionManager, Heartbeat
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


class HazelcastClient(object):
    logger = logging.getLogger("HazelcastClient")
    _config = None

    def __init__(self, config=None):
        self.config = config or ClientConfig()
        self.lifecycle = LifecycleService(self.config)
        self.reactor = AsyncoreReactor()
        self.connection_manager = ConnectionManager(self, self.reactor.new_connection)
        self.heartbeat = Heartbeat(self)
        self.invoker = InvocationService(self)
        self.listener = ListenerService(self)
        self.cluster = ClusterService(self.config, self)
        self.partition_service = PartitionService(self)
        self.proxy = ProxyManager(self)
        self.load_balancer = RandomLoadBalancer(self.cluster)
        self.serialization_service = SerializationServiceV1(serialization_config=self.config.serialization_config)
        self.transaction_manager = TransactionManager(self)
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
        return self.proxy.get_or_create(ATOMIC_LONG_SERVICE, name)

    def get_atomic_reference(self, name):
        return self.proxy.get_or_create(ATOMIC_REFERENCE_SERVICE, name)

    def get_count_down_latch(self, name):
        return self.proxy.get_or_create(COUNT_DOWN_LATCH_SERVICE, name)

    def get_executor(self, name):
        return self.proxy.get_or_create(EXECUTOR_SERVICE, name)

    def get_id_generator(self, name):
        atomic_long = self.get_atomic_long(ID_GENERATOR_ATOMIC_LONG_PREFIX + name)
        return self.proxy.get_or_create(ID_GENERATOR_SERVICE, name, atomic_long=atomic_long)

    def get_queue(self, name):
        return self.proxy.get_or_create(QUEUE_SERVICE, name)

    def get_list(self, name):
        return self.proxy.get_or_create(LIST_SERVICE, name)

    def get_lock(self, name):
        return self.proxy.get_or_create(LOCK_SERVICE, name)

    def get_map(self, name):
        return self.proxy.get_or_create(MAP_SERVICE, name)

    def get_multi_map(self, name):
        return self.proxy.get_or_create(MULTI_MAP_SERVICE, name)

    def get_reliable_topic(self, name):
        return self.proxy.get_or_create(RELIABLE_TOPIC_SERVICE, name)

    def get_replicated_map(self, name):
        return self.proxy.get_or_create(REPLICATED_MAP_SERVICE, name)

    def get_ringbuffer(self, name):
        return self.proxy.get_or_create(RINGBUFFER_SERIVCE, name)

    def get_semaphore(self, name):
        return self.proxy.get_or_create(SEMAPHORE_SERVICE, name)

    def get_set(self, name):
        return self.proxy.get_or_create(SET_SERVICE, name)

    def get_topic(self, name):
        return self.proxy.get_or_create(TOPIC_SERVICE, name)

    def new_transaction(self, timeout=120, durability=1, type=TWO_PHASE):
        return self.transaction_manager.new_transaction(timeout, durability, type)

    def shutdown(self):
        if self.lifecycle.is_live:
            self.lifecycle.fire_lifecycle_event(LIFECYCLE_STATE_SHUTTING_DOWN)
            self.partition_service.shutdown()
            self.heartbeat.shutdown()
            self.cluster.shutdown()
            self.reactor.shutdown()
            self.lifecycle.fire_lifecycle_event(LIFECYCLE_STATE_SHUTDOWN)
            self.logger.info("Client shutdown.")
