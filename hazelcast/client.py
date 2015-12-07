from connection import ConnectionManager, InvocationService
from cluster import ClusterService, RandomLoadBalancer
from partition import PartitionService
from proxy import ProxyManager, MAP_SERVICE

class HazelcastClient(object):
    _config = None

    def __init__(self, config=None):
        self._config = config
        self.invoker = InvocationService(self)
        self.connection_manager = ConnectionManager(self, self.invoker.handle_client_message)
        self.cluster = ClusterService(config, self)
        self.partition_service = PartitionService(self)
        self.proxy = ProxyManager(self)
        self.load_balancer = RandomLoadBalancer(self)

        self.cluster.start()
        self.partition_service.start()

    def get_map(self, name):
        return self.proxy.get_or_create(MAP_SERVICE, name)

class Config:
    def __init__(self):
        self.username = "dev"
        self.password = "dev-pass"
        self.addresses = []