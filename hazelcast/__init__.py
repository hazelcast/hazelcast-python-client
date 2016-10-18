from hazelcast.client import HazelcastClient
from hazelcast.config import ClientConfig, ClientNetworkConfig, SerializationConfig, GroupConfig

__version_info__ = (3, 7) # next release (3,7,1)
__version__ = '.'.join(map(str, __version_info__))
