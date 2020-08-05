import os

from tests.base import HazelcastTestCase
from tests.util import configure_logging, get_abs_path, random_string
from hazelcast.serialization.predicate import PagingPredicate, is_greater_than_or_equal_to
from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast import HazelcastClient
from hazelcast.six import assertCountEqual
from hazelcast.core import Comparator
from examples.serialization.identified_data_serializable_example import Student


class MapPagingPredicateTest(HazelcastTestCase):

    @classmethod
    def setUpClass(cls):
        configure_logging()

    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc, self._configure_cluster())
        self.member1 = self.cluster.start_member()
        self.member2 = self.cluster.start_member()
        self.client = HazelcastClient()
        self.map = self.client.get_map(random_string())

    def tearDown(self):
        self.map.destroy()
        self.client.shutdown()
        self.rc.exit()

    """
    Tests for proxy: comparator None
    """
    # Time out for assertTrueEventually set to 30. Could it be shorter?
    def test_entry_set_with_paging_predicate(self):
        self._fill_map()
        entry_set = self.map.entry_set(PagingPredicate(is_greater_than_or_equal_to('this', 3), 1)).result()

        def assert_event():
            self.assertEqual(len(entry_set), 1)
            assertCountEqual(self, entry_set[0], ['key-3', 3])

        self.assertTrueEventually(assert_event)

    def test_key_set_with_paging_predicate(self):
        self._fill_map()
        key_set = self.map.key_set(PagingPredicate(is_greater_than_or_equal_to('this', 3), 1)).result()

        def assert_event():
            self.assertEqual(len(key_set), 1)
            self.assertEqual(key_set[0], 'key-3')

        self.assertTrueEventually(assert_event)

    def test_values_with_paging_predicate(self):
        self._fill_map()
        values = self.map.values(PagingPredicate(is_greater_than_or_equal_to('this', 3), 1)).result()

        def assert_event():
            self.assertEqual(len(values), 1)
            self.assertEqual(values[0], 3)

        self.assertTrueEventually(assert_event)

    """
    Tests for paging: comparator None
    """
    def test_first_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        self.assertTrueEventually(lambda: assertCountEqual(self, self.map.values(paging).result(), [40, 41]))

    def test_next_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.next_page()
        self.assertTrueEventually(lambda: assertCountEqual(self, self.map.values(paging).result(), [42, 43]))

    def test_set_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(4)
        self.assertTrueEventually(lambda: assertCountEqual(self, self.map.values(paging).result(), [48, 49]))

    def test_get_page(self):
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(4)
        self.assertEqual(paging.get_page(), 4)

    def test_get_page_size(self):
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        self.assertEqual(paging.get_page_size(), 2)

    def test_previous_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(4)
        paging.previous_page()
        self.assertTrueEventually(lambda: assertCountEqual(self, self.map.values(paging).result(), [46, 47]))

    def test_get_4th_then_previous_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(4)
        self.map.values(paging)
        paging.previous_page()
        self.assertTrueEventually(lambda: assertCountEqual(self, self.map.values(paging).result(), [46, 47]))

    def test_get_3rd_then_next_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(3)
        self.map.values(paging)
        paging.next_page()
        self.assertTrueEventually(lambda: assertCountEqual(self, self.map.values(paging).result(), [48, 49]))

    def test_set_nonexistent_page(self):
        # Trying to get page 10, which is out of range, should return empty list
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(10)
        self.assertTrueEventually(lambda: assertCountEqual(self, self.map.values(paging).result(), []))

    def test_nonexistent_previous_page(self):
        # Trying to get previous page while already at first page should return first page.
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.previous_page()
        self.assertTrueEventually(lambda: assertCountEqual(self, self.map.values(paging).result(), [40, 41]))

    def test_nonexistent_next_page(self):
        # Trying to get next page while already at last page should return empty list.
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(4)
        paging.next_page()
        self.assertTrueEventually(lambda: assertCountEqual(self, self.map.values(paging).result(), []))

    def test_get_half_full_last_page(self):
        # Page size set to 2, but last page only has 1 element.
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 41), 2)
        paging.set_page(4)
        self.assertTrueEventually(lambda: assertCountEqual(self, self.map.values(paging).result(), [49]))

    def test_empty_map(self):
        # Empty map should return empty list
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 30), 2)
        assertCountEqual(self, self.map.values(paging).result(), [])

    # @HazelcastTestCase.skip('Paging predicate with equal values will be supported in Hazelcast 4.0')
    # def _test_equal_values_paging(self):
    #     self._fill_map_numeric()
    #
    #     # keys[50 - 99], values[0 - 49]:
    #     for i in range(50, 100):
    #         self.map.put(i, i - 50)
    #
    #     paging = PagingPredicate(is_less_than_or_equal_to('this', 8), 5)
    #     def assert_event():
    #         assertCountEqual(self, self.map.values(paging).result(), [0, 0, 1, 1, 2])
    #         paging.next_page()
    #         assertCountEqual(self, self.map.values(paging).result(), [2, 3, 3, 4, 4])
    #         paging.next_page()
    #         assertCountEqual(self, self.map.values(paging).result(), [5, 5, 6, 6, 7])
    #         paging.next_page()
    #         assertCountEqual(self, self.map.values(paging).result(), [7, 8, 8])
    #     self.assertTrueEventually(assert_event)

    """
    Test for paging predicate with custom comparator
    """
    def test_with_custom_comparator(self):
        # TODO: Register Student and StudentComparator factories on client and server side.
        self._fill_map_student()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 96), 2)
        self.assertTrueEventually(lambda: assertCountEqual(self, self.map.values(paging).result(), [100.0, 99.0]))
        paging.next_page()
        self.assertTrueEventually(lambda: assertCountEqual(self, self.map.values(paging).result(), [98.0, 97.0]))
        paging.next_page()
        self.assertTrueEventually(lambda: assertCountEqual(self, self.map.values(paging).result(), [96.0]))

    class StudentComparator(Comparator, IdentifiedDataSerializable):
        # How should I register this to the client side and Java side?
        FACTORY_ID = 2
        CLASS_ID = 1

        def compare(self, student1, student2):
            """
            Sort according to grade point average (gpa).
            :param student1: (studentID, Student) studentID is an int
            :param student2: (studentID, Student) studentID is an int
            :return: positive int if student1 has higher GPA, 0 if both GPAs equal,
            negative int if student2 has higher GPA
            """
            return student1.gpa - student2.gpa

        def get_factory_id(self):
            return self.FACTORY_ID

        def get_class_id(self):
            return self.CLASS_ID

        def read_data(self, object_data_input):
            pass

        def write_data(self, object_data_output):
            pass

    def _fill_map(self):
        self.map.put('key-1', 1)
        self.map.put('key-2', 2)
        self.map.put('key-3', 3)

    def _fill_map_numeric(self, count=50):
        for n in range(count):
            self.map.put(n, n)

    def _fill_map_student(self, count=10):
        for n in range(count):
            self.map.put(n, Student(id=count, gpa=100.0-count))

    def _configure_cluster(self):
        current_directory = os.path.dirname(__file__)
        with open(get_abs_path(current_directory, "hazelcast.xml"), "r") as f:
            return f.read()
