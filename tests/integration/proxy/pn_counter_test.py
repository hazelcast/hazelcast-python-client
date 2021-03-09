import os

from hazelcast import HazelcastClient
from hazelcast.errors import ConsistencyLostError
from tests.base import HazelcastTestCase
from tests.util import get_abs_path


class PNCounterConsistencyTest(HazelcastTestCase):
    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc, self._configure_cluster())
        self.cluster.start_member()
        self.cluster.start_member()
        self.client = HazelcastClient(cluster_name=self.cluster.id)
        self.pn_counter = self.client.get_pn_counter("pn-counter").blocking()

    def tearDown(self):
        self.client.shutdown()
        self.rc.terminateCluster(self.cluster.id)
        self.rc.exit()

    def test_consistency_lost_error_raised_when_target_terminates(self):
        self.pn_counter.add_and_get(3)

        replica_address = self.pn_counter._current_target_replica_address

        self.rc.terminateMember(self.cluster.id, str(replica_address.uuid))
        with self.assertRaises(ConsistencyLostError):
            self.pn_counter.add_and_get(5)

    def test_counter_can_continue_session_by_calling_reset(self):
        self.pn_counter.add_and_get(3)

        replica_address = self.pn_counter._current_target_replica_address

        self.rc.terminateMember(self.cluster.id, str(replica_address.uuid))
        self.pn_counter.reset()
        self.pn_counter.add_and_get(5)

    def _configure_cluster(self):
        current_directory = os.path.dirname(__file__)
        with open(
            get_abs_path(current_directory, "hazelcast_crdtreplication_delayed.xml"), "r"
        ) as f:
            return f.read()
