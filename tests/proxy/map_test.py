import unittest

from hzrc.client import HzRemoteController

import hazelcast
from tests.util import random_string


class ClientMapTest(unittest.TestCase):
    client = None
    rc = None

    @classmethod
    def setUpClass(cls):
        cls.rc = HzRemoteController('127.0.0.1', 9701)
        rc_cluster = cls.rc.createCluster(None, None)
        rc_member = cls.rc.startMember(rc_cluster.id)

        config = hazelcast.ClientConfig()
        config.network_config.addresses.append("{}:{}".format(rc_member.host, rc_member.port))
        cls.client = hazelcast.HazelcastClient(config)

    @classmethod
    def tearDownClass(cls):
        cls.client.shutdown()
        cls.rc.exit()

    def setUp(self):
        self.map = self.client.get_map(random_string())

    def tearDown(self):
        self.map.destroy()

    def fill_map(self, map):
        for i in xrange(0, 10):
            map.put("key-%d" % i, "value-%d" % i).result()

    def test_put_get(self):
        self.fill_map(self.map)
        for i in xrange(0, 10):
            self.assertEqual("value-%d" % i, self.map.get("key-%d" % i).result())

    def test_contains_key(self):
        self.fill_map(self.map)

        self.assertTrue(self.map.contains_key("key-1").result())
        self.assertFalse(self.map.contains_key("key-10").result())

    def test_size(self):
        self.fill_map(self.map)

        self.assertEqual(10, self.map.size().result())

    def test_remove(self):
        self.fill_map(self.map)

        self.map.remove("key-1").result()
        self.assertEqual(9, self.map.size().result())
        self.assertFalse(self.map.contains_key("key-1").result())

    def test_add_entry_listener(self):
        # is_item_added = False

        def item_added(event):
            # is_item_added = True
            print("item_added", event)

        self.map.add_entry_listener(include_value=True, added=item_added, removed=item_added)

        self.map.put('key', 'value').result()

        from time import sleep
        sleep(10)
