import os

from tests.base import SingleMemberTestCase
from tests.util import get_abs_path
from hazelcast.errors import NoDataMemberInClusterError


class PNCounterBasicTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

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

    def _check_pn_counter_method(self, return_value, expected_return_value, expected_get_value):
        get_value = self.pn_counter.get()

        self.assertEqual(expected_return_value, return_value)
        self.assertEqual(expected_get_value, get_value)


class PNCounterLiteMemberTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

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
