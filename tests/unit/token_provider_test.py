import unittest

from hazelcast.security import BasicTokenProvider


class BasicTokenProviderTestCase(unittest.TestCase):
    def test_empty(self):
        p = BasicTokenProvider()
        self.assertEqual(b"", p.token())

    def test_string(self):
        p = BasicTokenProvider("Hazelcast")
        self.assertEqual(b"Hazelcast", p.token())

    def test_bytes(self):
        p = BasicTokenProvider("Hazelcast".encode("utf-8"))
        self.assertEqual(b"Hazelcast", p.token())

    def test_invalid_type(self):
        self.assertRaises(TypeError, lambda: BasicTokenProvider(123456))
