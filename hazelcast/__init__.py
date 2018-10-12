from hazelcast.client import HazelcastClient
from hazelcast.config import ClientConfig, ClientNetworkConfig, SerializationConfig, GroupConfig, SSLConfig, \
    ClientCloudConfig

# version info determines release version
__version_info__ = (3, 10)
__version__ = '.'.join(map(str, __version_info__))
