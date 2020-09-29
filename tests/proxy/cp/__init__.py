import os

from hazelcast import HazelcastClient
from tests.base import HazelcastTestCase


class CPTestCase(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc, cls.configure_cluster())
        cls.cluster.start_member()
        cls.cluster.start_member()
        cls.cluster.start_member()
        cls.client = HazelcastClient(cluster_name=cls.cluster.id)

    @classmethod
    def tearDownClass(cls):
        cls.client.shutdown()
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    @classmethod
    def configure_cluster(cls):
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        with open(os.path.join(dir_path, "hazelcast_cpsubsystem.xml")) as f:
            return f.read()
