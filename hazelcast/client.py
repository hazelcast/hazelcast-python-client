import logging

from hazelcast.connection import ConnectionManager
from hazelcast.cluster import ClusterService, RandomLoadBalancer
from hazelcast.invocation import InvocationService
from hazelcast.reactor import AsyncoreConnection, AsyncoreReactor
from hazelcast.serialization import SerializationServiceV1
from hazelcast.partition import PartitionService
from hazelcast.proxy import ProxyManager, MAP_SERVICE, QUEUE_SERVICE

class HazelcastClient(object):
    logger = logging.getLogger("HazelcastClient")
    _config = None

    def __init__(self, config=None):
        self.config = config
        self.invoker = InvocationService(self)
        self.reactor = AsyncoreReactor()
        self.connection_manager = ConnectionManager(self, AsyncoreConnection)
        self.cluster = ClusterService(config, self)
        self.partition_service = PartitionService(self)
        self.proxy = ProxyManager(self)
        self.load_balancer = RandomLoadBalancer(self.cluster)
        self.serializer = SerializationServiceV1(self)

        self.reactor.start()
        self.cluster.start()
        self.partition_service.start()
        self.logger.info("Client started.")

    def get_map(self, name):
        return self.proxy.get_or_create(MAP_SERVICE, name)

    def get_queue(self, name):
        return self.proxy.get_or_create(QUEUE_SERVICE, name)

    def shutdown(self):
        self.partition_service.shutdown()
        self.cluster.shutdown()
        self.reactor.shutdown()
        self.logger.info("Client shutdown.")

class Config:
    def __init__(self):
        self.username = "dev"
        self.password = "dev-pass"
        self.addresses = []
