import logging
import time
import unittest
from threading import Thread
from tests.hzrc.client import HzRemoteController
import hazelcast
from hazelcast.core import Address
from tests.util import configure_logging


class _Member(object):
    def __init__(self, rc, cluster, member):
        self.rc, self.cluster, self.member = rc, cluster, member
        self.uuid = member.uuid
        self.address = Address(member.host, member.port)

    def shutdown(self):
        self.rc.terminateMember(self.cluster.id, self.member.uuid)


class _Cluster(object):
    def __init__(self, rc, cluster):
        self.cluster = cluster
        self.rc = rc
        self.id = cluster.id

    def start_member(self):
        return _Member(self.rc, self, self.rc.startMember(self.cluster.id))


class HazelcastTestCase(unittest.TestCase):
    clients = []

    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)
        self.logger = logging.getLogger(methodName)

    @staticmethod
    def create_rc():
        return HzRemoteController('127.0.0.1', 9701)

    @classmethod
    def create_cluster(cls, rc, config=None):
        return _Cluster(rc, rc.createCluster(None, config))

    def create_client(self, config=None):
        client = hazelcast.HazelcastClient(config)
        self.clients.append(client)
        return client

    def shutdown_all_clients(self):
        for c in self.clients:
            c.shutdown()
        self.clients = []

    def assertTrueEventually(self, assertion, timeout=30):
        timeout_time = time.time() + timeout
        while time.time() < timeout_time:
            try:
                assertion()
                return
            except AssertionError:
                time.sleep(0.1)
        raise

    def assertSetEventually(self, event, timeout=5):
        is_set = event.wait(timeout)
        self.assertTrue(is_set, "Event was not set within %d seconds" % timeout)

    def assertEntryEvent(self, event, event_type, key=None, value=None, old_value=None, merging_value=None,
                         number_of_affected_entries=1):

        self.assertEqual(event.key, key)
        self.assertEqual(event.event_type, event_type)
        self.assertEqual(event.value, value)
        self.assertEqual(event.merging_value, merging_value)
        self.assertEqual(event.old_value, old_value)
        self.assertEqual(event.number_of_affected_entries, number_of_affected_entries)

    def assertDistributedObjectEvent(self, event, name, service_name, event_type):
        self.assertEqual(name, event.name)
        self.assertEqual(service_name, event.service_name)
        self.assertEqual(event_type, event.event_type)

    def set_logging_level(self, level):
        logging.getLogger().setLevel(level)

    def start_new_thread(self, target):
        t = Thread(target=target)
        t.start()
        return t


class SingleMemberTestCase(HazelcastTestCase):
    """
    Test cases where a single member - client combination is needed
    """
    rc = None
    client = None

    @classmethod
    def setUpClass(cls):
        configure_logging()
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc, cls.configure_cluster())
        cls.member = cls.cluster.start_member()

        cls.client = hazelcast.HazelcastClient(cls.configure_client(hazelcast.ClientConfig()))

    @classmethod
    def tearDownClass(cls):
        cls.client.shutdown()
        cls.rc.exit()

    @classmethod
    def configure_client(cls, config):
        return config

    @classmethod
    def configure_cluster(cls):
        return None
