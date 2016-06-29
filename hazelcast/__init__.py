from hazelcast.client import HazelcastClient
from hazelcast.config import ClientConfig, ClientNetworkConfig, SerializationConfig, GroupConfig

__version_info__ = (0, 3, 0)
__version__ = '.'.join(map(str, __version_info__))
