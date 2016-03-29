import unittest

from hazelcast.config import SerializationConfig
from hazelcast.core import Address
from hazelcast.serialization.data import Data
from hazelcast.serialization.service import SerializationServiceV1


class SerializationTestCase(unittest.TestCase):
    def setUp(self):
        self.service = SerializationServiceV1(serialization_config=SerializationConfig())

    def tearDown(self):
        self.service.destroy()

    def test_test_dummy_encode_decode(self):
        obj = "Test obj"
        data = self.service.to_data(obj)

        obj2 = self.service.to_object(data)
        self.assertEqual(obj, obj2)

    def test_service_int(self):
        obj = 10
        data = self.service.to_data(obj)

        obj2 = self.service.to_object(data)
        self.assertEqual(obj, obj2)
        self.assertEqual(type(obj), type(obj2))

    def test_service_int_array(self):
        obj = [10, 20, 30]
        data = self.service.to_data(obj)

        obj2 = self.service.to_object(data)
        self.assertEqual(obj, obj2)

    def test_service_data(self):
        obj = "TEST"
        data = self.service.to_data(obj)

        self.assertEqual(16, len(data.to_bytes()))

    def test_python_pickle_serialization(self):
        obj = Address("localhost", 5701)
        data = self.service.to_data(obj)

        obj2 = self.service.to_object(data)
        self.assertEqual(obj, obj2)

    def test_null_data(self):
        data = Data()
        obj = self.service.to_object(data)
        self.assertIsNone(obj)

    def test_none_serialize(self):
        obj = None
        data = self.service.to_data(obj)
        self.assertIsNone(data)

    def test_serialize_data(self):
        data = Data()
        obj = self.service.to_data(data)
        self.assertTrue(isinstance(obj, Data))

    def test_not_data_deserialize(self):
        obj = 0
        obj2 = self.service.to_object(obj)
        self.assertEqual(obj, obj2)
