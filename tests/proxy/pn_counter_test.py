import os

from tests.base import SingleMemberTestCase, HazelcastTestCase
from tests.util import configure_logging, get_abs_path, set_attr
from hazelcast.exception import ConsistencyLostError, NoDataMemberInClusterError
from hazelcast import HazelcastClient


@set_attr(category=3.10)
class PNCounterBasicTest(SingleMemberTestCase):
    def setUp(self):
        self.pn_counter = self.client.get_pn_counter("pn-counter").blocking()

    def tearDown(self):
        self.pn_counter.destroy()

    def test_get(self):
        self.pn_counter.add_and_get(4)
        self.assertEqual(4, self.pn_counter.get())

    def test_get_initial_value(self):
        self.assertEqual(0, self.pn_counter.get())

    def test_get_and_add(self):
        self._check_pn_counter_method(self.pn_counter.get_and_add(3), 0, 3)

    def test_add_and_get(self):
        self._check_pn_counter_method(self.pn_counter.add_and_get(4), 4, 4)

    def test_get_and_subtract(self):
        self._check_pn_counter_method(self.pn_counter.get_and_subtract(2), 0, -2)

    def test_subtract_and_get(self):
        self._check_pn_counter_method(self.pn_counter.subtract_and_get(5), -5, -5)

    def test_get_and_decrement(self):
        self._check_pn_counter_method(self.pn_counter.get_and_decrement(), 0, -1)

    def test_decrement_and_get(self):
        self._check_pn_counter_method(self.pn_counter.decrement_and_get(), -1, -1)

    def test_get_and_increment(self):
        self._check_pn_counter_method(self.pn_counter.get_and_increment(), 0, 1)

    def test_increment_and_get(self):
        self._check_pn_counter_method(self.pn_counter.increment_and_get(), 1, 1)

    def test_reset(self):
        self.pn_counter.get_and_add(1)
        old_vector_clock = self.pn_counter._observed_clock
        self.pn_counter.reset()

        self.assertNotEqual(old_vector_clock, self.pn_counter._observed_clock)

    def _check_pn_counter_method(self, return_value, expected_return_value, expected_get_value):
        get_value = self.pn_counter.get()

        self.assertEqual(expected_return_value, return_value)
        self.assertEqual(expected_get_value, get_value)


@set_attr(category=3.10)
class PNCounterConsistencyTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        configure_logging()

    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc, self._configure_cluster())
        self.member1 = self.cluster.start_member()
        self.member2 = self.cluster.start_member()
        self.client = HazelcastClient()
        self.pn_counter = self.client.get_pn_counter("pn-counter").blocking()

    def tearDown(self):
        self.pn_counter.destroy()
        self.client.shutdown()
        self.rc.exit()

    def test_consistency_lost_error_raised_when_target_terminates(self):
        self.pn_counter.add_and_get(3)

        replica_address = self.pn_counter._current_target_replica_address
        member = self.client.cluster.get_member_by_address(replica_address)

        self.rc.terminateMember(self.cluster.id, member.uuid)
        with self.assertRaises(ConsistencyLostError):
            self.pn_counter.add_and_get(5)

    def test_counter_can_continue_session_by_calling_reset(self):
        self.pn_counter.add_and_get(3)

        replica_address = self.pn_counter._current_target_replica_address
        member = self.client.cluster.get_member_by_address(replica_address)

        self.rc.terminateMember(self.cluster.id, member.uuid)
        self.pn_counter.reset()
        self.pn_counter.add_and_get(5)

    def _configure_cluster(self):
        current_directory = os.path.dirname(__file__)
        with open(get_abs_path(current_directory, "hazelcast_crdtreplication_delayed.xml"), "r") as f:
            return f.read()


@set_attr(category=3.10)
class PNCounterLiteMemberTest(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        current_directory = os.path.dirname(__file__)
        with open(get_abs_path(current_directory, "hazelcast_litemember.xml"), "r") as f:
            return f.read()

    def setUp(self):
        self.pn_counter = self.client.get_pn_counter("pn-counter").blocking()

    def tearDown(self):
        self.pn_counter.destroy()

    def test_get_with_lite_member(self):
        self._verify_error_raised(NoDataMemberInClusterError, self.pn_counter.get)

    def test_get_and_add_with_lite_member(self):
        self._verify_error_raised(NoDataMemberInClusterError, self.pn_counter.get_and_add, 1)

    def test_add_and_get_with_lite_member(self):
        self._verify_error_raised(NoDataMemberInClusterError, self.pn_counter.add_and_get, 2)

    def test_get_and_subtract_with_lite_member(self):
        self._verify_error_raised(NoDataMemberInClusterError, self.pn_counter.get_and_subtract, 1)

    def test_subtract_and_get_with_lite_member(self):
        self._verify_error_raised(NoDataMemberInClusterError, self.pn_counter.subtract_and_get, 5)

    def test_get_and_decrement_with_lite_member(self):
        self._verify_error_raised(NoDataMemberInClusterError, self.pn_counter.get_and_decrement)

    def test_decrement_and_get_with_lite_member(self):
        self._verify_error_raised(NoDataMemberInClusterError, self.pn_counter.decrement_and_get)

    def test_get_and_increment(self):
        self._verify_error_raised(NoDataMemberInClusterError, self.pn_counter.get_and_increment)

    def test_increment_and_get(self):
        self._verify_error_raised(NoDataMemberInClusterError, self.pn_counter.increment_and_get)

    def _verify_error_raised(self, error, func, *args):
        with self.assertRaises(error):
            func(*args)
