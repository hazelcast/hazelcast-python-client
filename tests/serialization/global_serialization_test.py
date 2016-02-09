import cPickle as pickle
import unittest

from hazelcast.config import SerializationConfig, GlobalSerializerConfig, SerializerConfig
from hazelcast.serialization.api import StreamSerializer
from hazelcast.serialization.service import SerializationServiceV1


class TestGlobalSerializer(StreamSerializer):
    def __init__(self):
        super(TestGlobalSerializer, self).__init__()

    def read(self, inp):
        utf = inp.read_utf()
        return pickle.loads(utf.encode())

    def write(self, out, obj):
        out.write_utf(pickle.dumps(obj))

    def get_type_id(self):
        return 10000

    def destroy(self):
        pass


class CustomClass(object):
    def __init__(self, uid, name, text):
        self.uid = uid
        self.name = name
        self.text = text

    def __eq__(self, other):
        if other:
            return self.name == other.name and self.uid == other.uid and self.text == other.text
        return False


class CustomSerializer(StreamSerializer):
    def write(self, out, obj):
        if isinstance(obj, CustomClass):
            out.write_utf(obj.uid)
            out.write_utf(obj.name)
            out.write_utf(obj.text)
        else:
            raise ValueError("Can only serialize CustomClass")

    def read(self, inp):
        return CustomClass(inp.read_utf(),  # uid
                           inp.read_utf(),  # name
                           inp.read_utf())  # text

    def get_type_id(self):
        return 10001


class CustomSerializationTestCase(unittest.TestCase):

    def test_global_encode_decode(self):
        config = SerializationConfig()
        config.global_serializer_config = GlobalSerializerConfig(TestGlobalSerializer)

        service = SerializationServiceV1(serialization_config=config)
        obj = CustomClass("uid", "some name", "description text")
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assertEqual(obj, obj2)

    def test_custom_serializer(self):
        config = SerializationConfig()
        custom_serializer_config = SerializerConfig(CustomSerializer, CustomClass)
        config.serializer_configs.append(custom_serializer_config)

        service = SerializationServiceV1(serialization_config=config)
        obj = CustomClass("uid", "some name", "description text")
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assertEqual(obj, obj2)