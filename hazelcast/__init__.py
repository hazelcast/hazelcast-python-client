from hazelcast.client import HazelcastClient
from hazelcast.config import ClientConfig, ClientNetworkConfig, SerializationConfig, GroupConfig

# version info determines release version
__version_info__ = (3, 8, 1)
__version__ = '.'.join(map(str, __version_info__))
