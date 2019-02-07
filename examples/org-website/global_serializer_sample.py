import hazelcast

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


if __name__ == "__main__":
    config = ClientConfig()
    config.serialization_config.global_serializer = GlobalSerializer
    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient(config)
    # GlobalSerializer will serialize/deserialize all non-builtin types
    hz.shutdown()
