import cPickle as pickle
import unittest

from hazelcast.config import SerializationConfig, GlobalSerializerConfig
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


class GlobalSerializationTestCase(unittest.TestCase):
    def setUp(self):
        config = SerializationConfig()
        global_serializer_config = GlobalSerializerConfig()
        global_serializer_config.set_serializer(TestGlobalSerializer)

        config.global_serializer_config = global_serializer_config

        self.service = SerializationServiceV1(serialization_config=config)

    def test_encode_decode(self):
        obj = CustomClass("uid", "some name", "description text")
        data = self.service.to_data(obj)

        obj2 = self.service.to_object(data)
        self.assertEqual(obj, obj2)
