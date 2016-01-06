import logging

from hazelcast.config import ClientConfig
from hazelcast.cluster import ClusterService, RandomLoadBalancer
from hazelcast.connection import ConnectionManager
from hazelcast.invocation import InvocationService, ListenerService
from hazelcast.reactor import AsyncoreConnection, AsyncoreReactor
from hazelcast.partition import PartitionService
from hazelcast.proxy import ProxyManager, MAP_SERVICE, QUEUE_SERVICE
from hazelcast.serialization import SerializationServiceV1


class HazelcastClient(object):
    logger = logging.getLogger("HazelcastClient")
    _config = None

    def __init__(self, config=None):
        self.config = config or ClientConfig()
        self.reactor = AsyncoreReactor()
        self.connection_manager = ConnectionManager(self, AsyncoreConnection)
        self.invoker = InvocationService(self)
        self.listener = ListenerService(self)
        self.cluster = ClusterService(config, self)
        self.partition_service = PartitionService(self)
        self.proxy = ProxyManager(self)
        self.load_balancer = RandomLoadBalancer(self.cluster)
        self.serializer = SerializationServiceV1(serialization_config=config.serialization_config)

        self.reactor.start()
        try:
            self.cluster.start()
            self.partition_service.start()
        except:
            import sys
            self.reactor.shutdown()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
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


