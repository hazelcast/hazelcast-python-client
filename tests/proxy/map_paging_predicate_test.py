import os
import unittest

from tests.base import HazelcastTestCase
from tests.util import configure_logging, get_abs_path, random_string
from hazelcast.serialization.predicate import paging, is_greater_than_or_equal_to, is_less_than_or_equal_to, \
    is_ilike, true
from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast import HazelcastClient
from hazelcast.util import ITERATION_TYPE
from hazelcast.core import Comparator


class MapPagingPredicateTest(HazelcastTestCase):

    @staticmethod
    def _configure_cluster():
        current_directory = os.path.dirname(__file__)
        with open(get_abs_path(current_directory, "hazelcast.xml"), "r") as f:
            return f.read()

    @classmethod
    def setUpClass(cls):
        configure_logging()
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc, MapPagingPredicateTest._configure_cluster())
        cls.member1 = cls.cluster.start_member()
        cls.member2 = cls.cluster.start_member()
        cls.client = HazelcastClient()
        cls.map = cls.client.get_map(random_string()).blocking()

    def setUp(self):
        self.map.clear()

    @classmethod
    def tearDownClass(cls):
        cls.map.destroy()
        cls.client.shutdown()
        cls.rc.exit()

    def test_with_inner_paging_predicate(self):
        predicate = paging(true(), 1)

        with self.assertRaises(TypeError):
            paging(predicate, 1)

    def test_with_non_positive_page_size(self):
        with self.assertRaises(ValueError):
            paging(true(), 0)

        with self.assertRaises(ValueError):
            paging(true(), -1)

    def test_previous_page_when_index_is_zero(self):
        predicate = paging(true(), 2)
        self.assertEqual(0, predicate.previous_page())
        self.assertEqual(0, predicate.previous_page())

    """
    Tests for proxy: comparator None
    """
    def test_entry_set_with_paging_predicate(self):
        self._fill_map_simple()
        entry_set = self.map.entry_set(paging(is_greater_than_or_equal_to('this', 3), 1))
        self.assertEqual(len(entry_set), 1)
        self.assertEqual(entry_set[0], ('key-3', 3))

    def test_key_set_with_paging_predicate(self):
        self._fill_map_simple()
        key_set = self.map.key_set(paging(is_greater_than_or_equal_to('this', 3), 1))
        self.assertEqual(len(key_set), 1)
        self.assertEqual(key_set[0], 'key-3')

    def test_values_with_paging_predicate(self):
        self._fill_map_simple()
        values = self.map.values(paging(is_greater_than_or_equal_to('this', 3), 1))
        self.assertEqual(len(values), 1)
        self.assertEqual(values[0], 3)

    """
    Tests for paging: comparator None
    """
    def test_first_page(self):
        self._fill_map_numeric()
        predicate = paging(is_greater_than_or_equal_to('this', 40), 2)
        self.assertEqual(self.map.values(predicate), [40, 41])

    def test_next_page(self):
        self._fill_map_numeric()
        predicate = paging(is_greater_than_or_equal_to('this', 40), 2)
        predicate.next_page()
        self.assertEqual(self.map.values(predicate), [42, 43])

    def test_set_page(self):
        self._fill_map_numeric()
        predicate = paging(is_greater_than_or_equal_to('this', 40), 2)
        predicate.page = 4
        self.assertEqual(self.map.values(predicate), [48, 49])

    def test_get_page(self):
        predicate = paging(is_greater_than_or_equal_to('this', 40), 2)
        predicate.page = 4
        self.assertEqual(predicate.page, 4)

    def test_page_size(self):
        predicate = paging(is_greater_than_or_equal_to('this', 40), 2)
        self.assertEqual(predicate.page_size, 2)

    def test_previous_page(self):
        self._fill_map_numeric()
        predicate = paging(is_greater_than_or_equal_to('this', 40), 2)
        predicate.page = 4
        predicate.previous_page()
        self.assertEqual(self.map.values(predicate), [46, 47])

    def test_get_4th_then_previous_page(self):
        self._fill_map_numeric()
        predicate = paging(is_greater_than_or_equal_to('this', 40), 2)
        predicate.page = 4
        self.map.values(predicate)
        predicate.previous_page()
        self.assertEqual(self.map.values(predicate), [46, 47])

    def test_get_3rd_then_next_page(self):
        self._fill_map_numeric()
        predicate = paging(is_greater_than_or_equal_to('this', 40), 2)
        predicate.page = 3
        self.map.values(predicate)
        predicate.next_page()
        self.assertEqual(self.map.values(predicate), [48, 49])

    def test_set_nonexistent_page(self):
        # Trying to get page 10, which is out of range, should return empty list.
        self._fill_map_numeric()
        predicate = paging(is_greater_than_or_equal_to('this', 40), 2)
        predicate.page = 10
        self.assertEqual(self.map.values(predicate), [])

    def test_nonexistent_previous_page(self):
        # Trying to get previous page while already at first page should return first page.
        self._fill_map_numeric()
        predicate = paging(is_greater_than_or_equal_to('this', 40), 2)
        predicate.previous_page()
        self.assertEqual(self.map.values(predicate), [40, 41])

    def test_nonexistent_next_page(self):
        # Trying to get next page while already at last page should return empty list.
        self._fill_map_numeric()
        predicate = paging(is_greater_than_or_equal_to('this', 40), 2)
        predicate.page = 4
        predicate.next_page()
        self.assertEqual(self.map.values(predicate), [])

    def test_get_half_full_last_page(self):
        # Page size set to 2, but last page only has 1 element.
        self._fill_map_numeric()
        predicate = paging(is_greater_than_or_equal_to('this', 41), 2)
        predicate.page = 4
        self.assertEqual(self.map.values(predicate), [49])

    def test_reset(self):
        self._fill_map_numeric()
        predicate = paging(is_greater_than_or_equal_to('this', 40), 2)
        self.assertEqual(self.map.values(predicate), [40, 41])
        predicate.next_page()
        self.assertEqual(self.map.values(predicate), [42, 43])
        predicate.reset()
        self.assertEqual(self.map.values(predicate), [40, 41])

    def test_empty_map(self):
        # Empty map should return empty list.
        predicate = paging(is_greater_than_or_equal_to('this', 30), 2)
        self.assertEqual(self.map.values(predicate), [])

    @unittest.skip('Paging predicate with duplicate values will be supported in Hazelcast 4.0')
    def test_equal_values_paging(self):
        self._fill_map_numeric()

        # keys[50 - 99], values[0 - 49]:
        for i in range(50, 100):
            self.map.put(i, i - 50)

        predicate = paging(is_less_than_or_equal_to('this', 8), 5)

        self.assertEqual(self.map.values(predicate), [0, 0, 1, 1, 2])
        predicate.next_page()
        self.assertEqual(self.map.values(predicate), [2, 3, 3, 4, 4])
        predicate.next_page()
        self.assertEqual(self.map.values(predicate), [5, 5, 6, 6, 7])
        predicate.next_page()
        self.assertEqual(self.map.values(predicate), [7, 8, 8])

    """
    Test for paging predicate with custom comparator
    """
    def test_key_set_paging_with_custom_comparator(self):
        self._fill_map_custom_comp()
        custom_cmp = CustomComparator(type=1, iteration_type=ITERATION_TYPE.KEY)
        predicate = paging(is_ilike('__key', 'key-%'), 6, custom_cmp)

        key_set_page_1 = self.map.key_set(predicate)
        predicate.next_page()
        key_set_page_2 = self.map.key_set(predicate)
        predicate.next_page()
        key_set_page_3 = self.map.key_set(predicate)

        self.assertEqual(key_set_page_1, ['key-9', 'key-8', 'key-7', 'key-6', 'key-5', 'key-4'])
        self.assertEqual(key_set_page_2, ['key-3', 'key-2', 'key-1', 'key-0'])
        self.assertEqual(key_set_page_3, [])

    def test_values_paging_with_custom_comparator(self):
        self._fill_map_custom_comp_2()
        custom_cmp = CustomComparator(type=2, iteration_type=ITERATION_TYPE.VALUE)
        predicate = paging(None, 6, custom_cmp)

        values_page_1 = self.map.values(predicate)
        predicate.next_page()
        values_page_2 = self.map.values(predicate)
        predicate.next_page()
        values_page_3 = self.map.values(predicate)

        self.assertEqual(values_page_1, ['A', 'BB', 'CCC', 'DDDD', 'EEEEE', 'FFFFFF'])
        self.assertEqual(values_page_2, ['GGGGGGG', 'HHHHHHHH', 'IIIIIIIII', 'JJJJJJJJJJ'])
        self.assertEqual(values_page_3, [])

    def test_entry_set_paging_with_custom_comparator(self):
        self._fill_map_custom_comp_2()
        custom_cmp = CustomComparator(type=2, iteration_type=ITERATION_TYPE.ENTRY)
        predicate = paging(None, 2, custom_cmp)
        page1 = self.map.entry_set(predicate)

        self.assertEqual(page1, [('key-65', 'A'), ('key-66', 'BB')])

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
