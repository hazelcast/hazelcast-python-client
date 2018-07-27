import hazelcast
import logging

from hazelcast import ClientConfig
from hazelcast.serialization.api import StreamSerializer


class GlobalSerializer(StreamSerializer):
    def write(self, out, obj):
        # out.write_byte_array(MyFavoriteSerializer.serialize(obj))
        pass

    def read(self, inp):
        # return MyFavoriteSerializer.deserialize(inp)
        return None

    def get_type_id(self):
        return 20

    def destroy(self):
        pass


class CustomSerializableType(object):
    def __init__(self, value=None):
        self.value = value


if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    config = ClientConfig()
    config.serialization_config.global_serializer = GlobalSerializer
    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient(config)
    # GlobalSerializer will serialize/deserialize all non-builtin types

    # Shutdown this Hazelcast Client
    hz.shutdown()
