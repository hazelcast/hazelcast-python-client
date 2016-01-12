import unittest

from hazelcast.config import SerializationConfig
from hazelcast.serialization.service import SerializationServiceV1

from ..util import random_string


class SerializationTestCase(unittest.TestCase):
    def setUp(self):
        n = random_string()
        self.service = SerializationServiceV1(serialization_config=SerializationConfig())

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


if __name__ == '__main__':
    unittest.main()
