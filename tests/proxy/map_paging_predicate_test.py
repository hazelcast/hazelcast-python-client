import os
import unittest

from tests.base import HazelcastTestCase
from tests.util import configure_logging, get_abs_path, random_string
from hazelcast.serialization.predicate import PagingPredicate, is_greater_than_or_equal_to, is_less_than_or_equal_to, \
    is_ilike
from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast import HazelcastClient
from hazelcast.six import assertCountEqual
from hazelcast.util import ITERATION_TYPE
from hazelcast.core import Comparator


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
    def test_entry_set_with_paging_predicate(self):
        self._fill_map_simple()
        entry_set = self.map.entry_set(PagingPredicate(is_greater_than_or_equal_to('this', 3), 1)).result()

        def assert_event():
            self.assertEqual(len(entry_set), 1)
            assertCountEqual(self, entry_set[0], ['key-3', 3])

        self.assertTrueEventually(assert_event, 5)

    def test_key_set_with_paging_predicate(self):
        self._fill_map_simple()
        key_set = self.map.key_set(PagingPredicate(is_greater_than_or_equal_to('this', 3), 1)).result()

        def assert_event():
            self.assertEqual(len(key_set), 1)
            self.assertEqual(key_set[0], 'key-3')

        self.assertTrueEventually(assert_event, 5)

    def test_values_with_paging_predicate(self):
        self._fill_map_simple()
        values = self.map.values(PagingPredicate(is_greater_than_or_equal_to('this', 3), 1)).result()

        def assert_event():
            self.assertEqual(len(values), 1)
            self.assertEqual(values[0], 3)

        self.assertTrueEventually(assert_event, 5)

    """
    Tests for paging: comparator None
    """
    def test_first_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)

        def assert_event():
            assertCountEqual(self, self.map.values(paging).result(), [40, 41])

        self.assertTrueEventually(assert_event, 5)

    def test_next_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.next_page()

        def assert_event():
            assertCountEqual(self, self.map.values(paging).result(), [42, 43])

        self.assertTrueEventually(assert_event, 5)

    def test_set_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(4)

        def assert_event():
            assertCountEqual(self, self.map.values(paging).result(), [48, 49])

        self.assertTrueEventually(assert_event, 5)

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

        def assert_event():
            assertCountEqual(self, self.map.values(paging).result(), [46, 47])

        self.assertTrueEventually(assert_event, 5)

    def test_get_4th_then_previous_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(4)
        self.map.values(paging)
        paging.previous_page()

        def assert_event():
            assertCountEqual(self, self.map.values(paging).result(), [46, 47])

        self.assertTrueEventually(assert_event, 5)

    def test_get_3rd_then_next_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(3)
        self.map.values(paging)
        paging.next_page()

        def assert_event():
            assertCountEqual(self, self.map.values(paging).result(), [48, 49])

        self.assertTrueEventually(assert_event, 5)

    def test_set_nonexistent_page(self):
        # Trying to get page 10, which is out of range, should return empty list.
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(10)

        def assert_event():
            assertCountEqual(self, self.map.values(paging).result(), [])

        self.assertTrueEventually(assert_event, 5)

    def test_nonexistent_previous_page(self):
        # Trying to get previous page while already at first page should return first page.
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.previous_page()

        def assert_event():
            assertCountEqual(self, self.map.values(paging).result(), [40, 41])

        self.assertTrueEventually(assert_event, 5)

    def test_nonexistent_next_page(self):
        # Trying to get next page while already at last page should return empty list.
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(4)
        paging.next_page()

        def assert_event():
            assertCountEqual(self, self.map.values(paging).result(), [])

        self.assertTrueEventually(assert_event, 5)

    def test_get_half_full_last_page(self):
        # Page size set to 2, but last page only has 1 element.
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 41), 2)
        paging.set_page(4)

        def assert_event():
            assertCountEqual(self, self.map.values(paging).result(), [49])

        self.assertTrueEventually(assert_event, 5)

    def test_empty_map(self):
        # Empty map should return empty list.
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 30), 2)
        assertCountEqual(self, self.map.values(paging).result(), [])

    @unittest.skip('Paging predicate with duplicate values will be supported in Hazelcast 4.0')
    def _test_equal_values_paging(self):
        self._fill_map_numeric()

        # keys[50 - 99], values[0 - 49]:
        for i in range(50, 100):
            self.map.put(i, i - 50)

        paging = PagingPredicate(is_less_than_or_equal_to('this', 8), 5)

        def assert_event():
            assertCountEqual(self, self.map.values(paging).result(), [0, 0, 1, 1, 2])
            paging.next_page()
            assertCountEqual(self, self.map.values(paging).result(), [2, 3, 3, 4, 4])
            paging.next_page()
            assertCountEqual(self, self.map.values(paging).result(), [5, 5, 6, 6, 7])
            paging.next_page()
            assertCountEqual(self, self.map.values(paging).result(), [7, 8, 8])

        self.assertTrueEventually(assert_event, 5)

    """
    Test for paging predicate with custom comparator
    """
    def test_key_set_paging_with_custom_comparator(self):
        self._fill_map_custom_comp()
        custom_cmp = CustomComparator(type=1, iteration_type=ITERATION_TYPE.KEY)
        paging = PagingPredicate(is_ilike('__key', 'key-%'), 6, custom_cmp)

        key_set_page_1 = self.map.key_set(paging).result()
        paging.next_page()
        key_set_page_2 = self.map.key_set(paging).result()
        paging.next_page()
        key_set_page_3 = self.map.key_set(paging).result()

        def assert_event():
            assertCountEqual(self, key_set_page_1, ['key-9', 'key-8', 'key-7', 'key-6', 'key-5', 'key-4'])
            assertCountEqual(self, key_set_page_2, ['key-3', 'key-2', 'key-1', 'key-0'])
            assertCountEqual(self, key_set_page_3, [])

        self.assertTrueEventually(assert_event, 5)

    def test_values_paging_with_custom_comparator(self):
        self._fill_map_custom_comp_2()
        custom_cmp = CustomComparator(type=2, iteration_type=ITERATION_TYPE.VALUE)
        paging = PagingPredicate(None, 6, custom_cmp)

        values_page_1 = self.map.values(paging).result()
        paging.next_page()
        values_page_2 = self.map.values(paging).result()
        paging.next_page()
        values_page_3 = self.map.values(paging).result()

        def assert_event():
            assertCountEqual(self, values_page_1, ['A', 'BB', 'CCC', 'DDDD', 'EEEEE', 'FFFFFF'])
            assertCountEqual(self, values_page_2, ['GGGGGGG', 'HHHHHHHH', 'IIIIIIIII', 'JJJJJJJJJJ'])
            assertCountEqual(self, values_page_3, [])

        self.assertTrueEventually(assert_event, 5)

    def test_entry_set_paging_with_custom_comparator(self):
        self._fill_map_custom_comp_2()
        custom_cmp = CustomComparator(type=2, iteration_type=ITERATION_TYPE.ENTRY)
        paging = PagingPredicate(None, 2, custom_cmp)

        page1 = self.map.entry_set(paging).result()

        def assert_event():
            assertCountEqual(self, page1, [('key-65', 'A'), ('key-66', 'BB')])

        self.assertTrueEventually(assert_event, 5)

    def _fill_map_simple(self):
        self.map.put_all({
            'key-1': 1,
            'key-2': 2,
            'key-3': 3,
        })

    def _fill_map_numeric(self, count=50):
        self.map.put_all({i: i for i in range(count)})

    def _fill_map_custom_comp(self, count=10):
        self.map.put_all({'key-'+str(i): 'value-'+str(i) for i in range(count)})
        self.map.put_all({'keyx-' + str(i): 'valuex-' + str(i) for i in range(count)})

    def _fill_map_custom_comp_2(self):
        self.map.put_all({'key-'+str(i): chr(i)*(i-64) for i in range(65, 75)})

    def _configure_cluster(self):
        current_directory = os.path.dirname(__file__)
        with open(get_abs_path(current_directory, "hazelcast.xml"), "r") as f:
            return f.read()


class CustomComparator(Comparator, IdentifiedDataSerializable):
    """
    Custom serializable Comparator to test paging predicate in non java clients.
    """
    FACTORY_ID = 66  # IdentifiedFactory
    CLASS_ID = 2

    def __init__(self, type=0, iteration_type=ITERATION_TYPE.KEY):
        self._type = type
        self._iteration_type = iteration_type

    def compare(self, entry1, entry2):
        """
        This method is used to determine order of entries when sorting.
        - If return value is a negative value, [entry1] comes after [entry2],
        - If return value is a positive value, [entry1] comes before [entry2],
        - If return value is 0, [entry1] and [entry2] are indistinguishable in this sorting mechanism.
            Their order with respect to each other is undefined.
        This method must always return the same result given the same pair of entries.
        :param entry1: (K,V pair), first entry
        :param entry2: (K,V pair), second entry
        :return: (int), order index
        """
        if self._iteration_type == ITERATION_TYPE.ENTRY:
            str1 = str(entry1[0]) + str(entry1[1])
            str2 = str(entry2[0]) + str(entry2[1])
        elif self._iteration_type == ITERATION_TYPE.VALUE:
            str1 = str(entry1[1])
            str2 = str(entry2[1])
        else:
            # iteration_type is ITERATION_TYPE.KEY or default
            str1 = str(entry1[0])
            str2 = str(entry2[0])

        if self._type == 0:
            if str1 > str2:
                return 1
            else:
                return -1 if str1 < str2 else 0
        elif self._type == 1:
            if str2 > str1:
                return 1
            else:
                return -1 if str2 < str1 else 0
        elif self._type == 2:
            if len(str1) > len(str2):
                return 1
            else:
                return -1 if len(str1) < len(str2) else 0
        else:
            return 0

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def read_data(self, object_data_input):
        self._type = object_data_input.read_int()
        self._iteration_type = ITERATION_TYPE(object_data_input.read_int())

    def write_data(self, object_data_output):
        object_data_output.write_int(self._type)
        object_data_output.write_int(self._iteration_type)
