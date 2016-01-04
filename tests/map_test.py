import unittest
import hazelcast
from util import random_string


class ClientMapTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = hazelcast.ClientConfig()
        config.network_config.addresses.append("127.0.0.1:5701")
        cls.client = hazelcast.HazelcastClient(config)

    @classmethod
    def tearDownClass(cls):
        cls.client.shutdown()

    def setUp(self):
        self.map = self.client.get_map(random_string())

    def tearDown(self):
        self.map.destroy()

    def fill_map(self, map):
        for i in xrange(0, 10):
            map.put("key-%d" % i, "value-%d" % i)

    def test_put_get(self):
        self.fill_map(self.map)
        for i in xrange(0, 10):
            self.assertEqual("value-%d" % i, self.map.get("key-%d" % i))

    def test_contains_key(self):
        self.fill_map(self.map)

        self.assertTrue(self.map.contains_key("key-1"))
        self.assertFalse(self.map.contains_key("key-10"))

    def test_size(self):
        self.fill_map(self.map)

        self.assertEqual(10, self.map.size())

    def test_remove(self):
        self.fill_map(self.map)

        self.map.remove("key-1")
        self.assertEqual(9, self.map.size())
        self.assertFalse(self.map.contains_key("key-1"))
