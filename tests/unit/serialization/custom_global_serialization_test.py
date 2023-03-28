import pickle
import unittest

from hazelcast.config import Config
from hazelcast.serialization.api import StreamSerializer
from hazelcast.serialization.service import SerializationServiceV1


class GlobalSerializer(StreamSerializer):
    def __init__(self):
        super(GlobalSerializer, self).__init__()

    def read(self, inp):
        string = inp.read_string()
        obj = pickle.loads(string.encode())
        try:
            obj.source = "GLOBAL"
        except AttributeError:
            pass
        return obj

    def write(self, out, obj):
        out.write_string(pickle.dumps(obj, 0).decode("utf-8"))

    def get_type_id(self):
        return 10000

    def destroy(self):
        pass


class CustomClass:
    def __init__(self, uid, name, text, source=None):
        self.uid = uid
        self.name = name
        self.text = text
        self.source = source

    def __eq__(self, other):
        if other:
            return self.name == other.name and self.uid == other.uid and self.text == other.text
        return False


class ChildCustomClass(CustomClass):
    pass


class CustomSerializer(StreamSerializer):
    def write(self, out, obj):
        if isinstance(obj, CustomClass):
            out.write_string(obj.uid)
            out.write_string(obj.name)
            out.write_string(obj.text)
        else:
            raise ValueError("Can only serialize CustomClass")

    def read(self, inp):
        return CustomClass(inp.read_string(), inp.read_string(), inp.read_string(), "CUSTOM")

    def get_type_id(self):
        return 10001

    def destroy(self):
        pass


class TheOtherCustomSerializer(StreamSerializer):
    def write(self, out, obj):
        if isinstance(obj, CustomClass):
            out.write_string(obj.text)
            out.write_string(obj.name)
            out.write_string(obj.uid)
        else:
            raise ValueError("Can only serialize CustomClass")

    def read(self, inp):
        text_ = inp.read_string()
        name_ = inp.read_string()
        uid = inp.read_string()
        return CustomClass(uid, name_, text_, "CUSTOM")

    def get_type_id(self):
        return 10001

    def destroy(self):
        pass


class CustomSerializationTestCase(unittest.TestCase):
    def test_global_encode_decode(self):
        config = Config()
        config.global_serializer = GlobalSerializer

        service = SerializationServiceV1(config)
        obj = CustomClass("uid", "some name", "description text")
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assertEqual(obj, obj2)
        self.assertEqual("GLOBAL", obj2.source)

    def test_custom_serializer(self):
        config = Config()
        config.custom_serializers = {CustomClass: CustomSerializer}

        service = SerializationServiceV1(config)
        obj = CustomClass("uid", "some name", "description text")
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assertEqual(obj, obj2)
        self.assertEqual("CUSTOM", obj2.source)

    def test_global_custom_serializer(self):
        config = Config()
        config.custom_serializers = {CustomClass: CustomSerializer}
        config.global_serializer = GlobalSerializer

        service = SerializationServiceV1(config)
        obj = CustomClass("uid", "some name", "description text")
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assertEqual(obj, obj2)
        self.assertEqual("CUSTOM", obj2.source)

    def test_double_register_custom_serializer(self):
        config = Config()
        config.custom_serializers = {CustomClass: CustomSerializer}
        service = SerializationServiceV1(config)

        with self.assertRaises(ValueError):
            service._registry.safe_register_serializer(TheOtherCustomSerializer, CustomClass)

    def test_serializing_class_instances(self):
        config = Config()
        config.custom_serializers = {CustomClass: CustomSerializer}
        service = SerializationServiceV1(config)

        for clazz in (CustomClass, ChildCustomClass):
            data = service.to_data(clazz)
            deserialized = service.to_object(data)
            self.assertEqual(clazz, deserialized)

    def test_serializing_child_class_instances_with_super_class_serializer(self):
        config = Config()
        config.custom_serializers = {CustomClass: CustomSerializer}
        service = SerializationServiceV1(config)

        obj = ChildCustomClass("uid", "some name", "description text", "CUSTOM")
        data = service.to_data(obj)
        deserialized = service.to_object(data)

        self.assertIsInstance(deserialized, CustomClass)
        self.assertEqual(obj, deserialized)
