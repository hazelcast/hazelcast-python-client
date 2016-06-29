import cPickle as pickle
import unittest

from hazelcast.config import SerializationConfig
from hazelcast.serialization.api import StreamSerializer
from hazelcast.serialization.service import SerializationServiceV1


class TestGlobalSerializer(StreamSerializer):
    def __init__(self):
        super(TestGlobalSerializer, self).__init__()

    def read(self, inp):
        utf = inp.read_utf()
        obj = pickle.loads(utf.encode())
        try:
            obj.source = "GLOBAL"
        except AttributeError:
            pass
        return obj

    def write(self, out, obj):
        out.write_utf(pickle.dumps(obj))

    def get_type_id(self):
        return 10000

    def destroy(self):
        pass


class CustomClass(object):
    def __init__(self, uid, name, text, source=None):
        self.uid = uid
        self.name = name
        self.text = text
        self.source = source

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
                           inp.read_utf(),  # text
                           "CUSTOM")  # Source

    def get_type_id(self):
        return 10001

    def destroy(self):
        pass


class TheOtherCustomSerializer(StreamSerializer):
    def write(self, out, obj):
        if isinstance(obj, CustomClass):
            out.write_utf(obj.text)
            out.write_utf(obj.name)
            out.write_utf(obj.uid)
        else:
            raise ValueError("Can only serialize CustomClass")

    def read(self, inp):
        text_ = inp.read_utf()
        name_ = inp.read_utf()
        uid = inp.read_utf()
        return CustomClass(uid, name_, text_, "CUSTOM")

    def get_type_id(self):
        return 10001

    def destroy(self):
        pass


class CustomSerializationTestCase(unittest.TestCase):
    def test_global_encode_decode(self):
        config = SerializationConfig()
        config.global_serializer = TestGlobalSerializer

        service = SerializationServiceV1(serialization_config=config)
        obj = CustomClass("uid", "some name", "description text")
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assertEqual(obj, obj2)
        self.assertEqual("GLOBAL", obj2.source)

    def test_custom_serializer(self):
        config = SerializationConfig()
        config.set_custom_serializer(CustomClass, CustomSerializer)

        service = SerializationServiceV1(serialization_config=config)
        obj = CustomClass("uid", "some name", "description text")
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assertEqual(obj, obj2)
        self.assertEqual("CUSTOM", obj2.source)

    def test_global_custom_serializer(self):
        config = SerializationConfig()
        config.set_custom_serializer(CustomClass, CustomSerializer)
        config.global_serializer = TestGlobalSerializer

        service = SerializationServiceV1(serialization_config=config)
        obj = CustomClass("uid", "some name", "description text")
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assertEqual(obj, obj2)
        self.assertEqual("CUSTOM", obj2.source)

    def test_double_register_custom_serializer(self):
        config = SerializationConfig()
        config.set_custom_serializer(CustomClass, CustomSerializer)
        service = SerializationServiceV1(serialization_config=config)

        with self.assertRaises(ValueError):
            service._registry.safe_register_serializer(TheOtherCustomSerializer, CustomClass)
