from hazelcast import HazelcastClient
from hazelcast.config import Config, NearCacheConfig

config = Config()
config.cluster_name = "a-cluster"
config.cluster_members = ["10.212.1.132:5701"]
config.ssl_enabled = True

near_cache_config = NearCacheConfig()
near_cache_config.time_to_live = 120
near_cache_config.max_idle = 60

config.near_caches = {
    "a-map": near_cache_config,
}

client = HazelcastClient(config)

# Do something with the client

client.shutdown()
