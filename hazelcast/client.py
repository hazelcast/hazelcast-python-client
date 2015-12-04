from connection import ConnectionManager, Invoker
from cluster import ClusterService, PartitionService

class HazelcastClient(object):
    _config = None

    def __init__(self, config=None):
        self._config = config
        self.invoker = Invoker(self)
        self.connection_manager = ConnectionManager(self.invoker.handle_client_message)
        self.cluster = ClusterService(config, self)
        self.partition_service = PartitionService(self)

        self.cluster.start()
        self.partition_service.start()

    def get_map(self, name):
        pass

class Config:
    def __init__(self):
        self.username = "dev"
        self.password = "dev-pass"
        self.addresses = []
