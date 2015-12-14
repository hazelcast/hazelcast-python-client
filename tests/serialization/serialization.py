import unittest

from hazelcast.serialization.service import *


class SerializationTestCase(unittest.TestCase):
    def test_test_dummy_encode_decode(self):
        service = SerializationServiceV1()
        obj = "Test obj"
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assertEqual(obj, obj2)

    def test_service_int(self):
        service = SerializationServiceV1()

        obj = 10
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assertEqual(obj, obj2)
        self.assertEqual(type(obj), type(obj2))

    def test_service_int_array(self):
        service = SerializationServiceV1()

        obj = [10, 20, 30]
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assertEqual(obj, obj2)

    def test_service_str(self):
        service = SerializationServiceV1()

        obj = "TEST"
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assertEqual(obj, obj2)

    def test_service_data(self):
        service = SerializationServiceV1()

        obj = "TEST"
        data = service.to_data(obj)

        self.assertEqual(16, len(data.to_bytes()))


if __name__ == '__main__':
    unittest.main()
