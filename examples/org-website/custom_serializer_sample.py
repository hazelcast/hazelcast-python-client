import hazelcast
import logging

from hazelcast.serialization.api import StreamSerializer


class CustomSerializableType(object):
    def __init__(self, value=None):
        self.value = value


class CustomSerializer(StreamSerializer):
    def write(self, out, obj):
        out.write_int(len(obj.value))
        out.write_from(obj.value)

    def read(self, inp):
        length = inp.read_int()
        result = bytearray(length)
        inp.read_into(result, 0, length)
        return CustomSerializableType(result.decode("utf-8"))

    def get_type_id(self):
        return 10

    def destroy(self):
        pass


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    config = hazelcast.ClientConfig()
    config.serialization_config.set_custom_serializer(CustomSerializableType, CustomSerializer)

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient(config)
    # CustomSerializer will serialize/deserialize CustomSerializable objects
    hz.shutdown()
