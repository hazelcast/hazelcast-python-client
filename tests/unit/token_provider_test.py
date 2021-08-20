import unittest

from hazelcast.security import BasicTokenProvider
from hazelcast.core import Address


class BasicTokenProviderTestCase(unittest.TestCase):
    def test_empty(self):
        p = BasicTokenProvider()
        self.assertEquals(b"", p.token(Address("1.2.3.4", 5701)))

    def test_string(self):
        p = BasicTokenProvider("Hazelcast")
        self.assertEquals(b"Hazelcast", p.token(Address("1.2.3.4", 5701)))

    def test_bytes(self):
        p = BasicTokenProvider("Hazelcast".encode("utf-8"))
        self.assertEquals(b"Hazelcast", p.token(Address("1.2.3.4", 5701)))

    def test_invalid_type(self):
        self.assertRaises(TypeError, lambda: BasicTokenProvider(123456))
