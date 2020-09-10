import hazelcast

from hazelcast.serialization.api import StreamSerializer


class CustomSerializableType(object):
    def __init__(self, value=None):
        self.value = value


class CustomSerializer(StreamSerializer):
    def write(self, out, obj):
        out.write_utf(obj.value)

    def read(self, inp):
        return CustomSerializableType(inp.read_utf())

    def get_type_id(self):
        return 10

    def destroy(self):
        pass


# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
hz = hazelcast.HazelcastClient(custom_serializers={
    CustomSerializableType: CustomSerializer
})

# CustomSerializer will serialize/deserialize CustomSerializable objects
hz.shutdown()
