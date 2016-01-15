import time
import unittest

from hzrc.client import HzRemoteController

import hazelcast
from hazelcast.core import Address
from tests.util import configure_logging


class _Member(object):
    def __init__(self, rc, cluster, member):
        self.rc, self.cluster, self.member = rc, cluster, member
        self.uuid = member.uuid
        self.address = Address(member.host, member.port)

    def terminate(self):
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

        cls.client = hazelcast.HazelcastClient(cls.configure_client())

    @classmethod
    def tearDownClass(cls):
        cls.client.shutdown()
        cls.rc.exit()

    @classmethod
    def configure_client(cls):
        return hazelcast.ClientConfig()

    @classmethod
    def configure_cluster(cls):
        return None
