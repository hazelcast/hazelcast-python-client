import unittest

from hazelcast.serialization.service import SerializationService


class SerializationTestCase(unittest.TestCase):
    def test_test_dummy_encode_decode(self):
        service = SerializationService(None)
        obj = "Test obj"
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assertEqual(obj, obj2)


if __name__ == '__main__':
    unittest.main()
