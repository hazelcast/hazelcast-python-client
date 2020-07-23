import os

from tests.base import HazelcastTestCase
from tests.util import configure_logging, get_abs_path, random_string
from hazelcast.serialization.predicate import PagingPredicate, is_greater_than_or_equal_to, is_less_than_or_equal_to
from hazelcast import HazelcastClient
from hazelcast.six import assertCountEqual


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
        self.map = self.client.get_map(random_string()).blocking()

    def tearDown(self):
        self.map.destroy()
        self.client.shutdown()
        self.rc.exit()

    """
    Tests for proxy: comparator None
    """
    def test_entry_set_with_paging_predicate(self):
        self._fill_map()
        entry_set = self.map.entry_set(PagingPredicate(is_greater_than_or_equal_to('this', 3), 1))
        self.assertEqual(len(entry_set), 1)
        assertCountEqual(self, entry_set[0], ['key-3', 3])

    def test_key_set_with_paging_predicate(self):
        self._fill_map()
        key_set = self.map.key_set(PagingPredicate(is_greater_than_or_equal_to('this', 3), 1))
        self.assertEqual(len(key_set), 1)
        self.assertEqual(key_set[0], 'key-3')

    def test_values_with_paging_predicate(self):
        self._fill_map()
        values = self.map.values(PagingPredicate(is_greater_than_or_equal_to('this', 3), 1))
        self.assertEqual(len(values), 1)
        self.assertEqual(values[0], 3)

    """
    Tests for paging: comparator None
    """
    def test_first_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        assertCountEqual(self, list(self.map.values(paging)), [40, 41])

    def test_next_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.next_page()
        assertCountEqual(self, list(self.map.values(paging)), [42, 43])

    def test_set_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(4)  # page index starts from 0 -> page 4 has 9th and 10th items. Correct behavior?
        assertCountEqual(self, list(self.map.values(paging)), [48, 49])

    def test_get_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(4)
        self.assertEqual(paging.get_page(), 4)

    def test_get_page_size(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        self.assertEqual(paging.get_page_size(), 2)

    def test_previous_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(4)
        paging.previous_page()
        assertCountEqual(self, list(self.map.values(paging)), [46, 47])

    def test_get_4th_then_previous_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(4)
        self.map.values(paging)
        paging.previous_page()
        assertCountEqual(self, list(self.map.values(paging)), [46, 47])

    def test_get_3rd_then_next_page(self):
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(3)
        self.map.values(paging)
        paging.next_page()
        assertCountEqual(self, list(self.map.values(paging)), [48, 49])

    def test_set_nonexistent_page(self):
        # Trying to get page 10, which is out of range, should return empty list
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(10)
        assertCountEqual(self, list(self.map.values(paging)), [])

    def test_nonexistent_previous_page(self):
        # Trying to get previous page while already at first page should return first page.
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.previous_page()
        assertCountEqual(self, list(self.map.values(paging)), [40, 41])

    def test_nonexistent_next_page(self):
        # Trying to get next page while already at last page should return empty list.
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 40), 2)
        paging.set_page(4)
        paging.next_page()
        assertCountEqual(self, list(self.map.values(paging)), [])

    def test_get_half_full_last_page(self):
        # Page size set to 2, but last page only has 1 element.
        self._fill_map_numeric()
        paging = PagingPredicate(is_greater_than_or_equal_to('this', 41), 2)
        paging.set_page(4)
        assertCountEqual(self, list(self.map.values(paging)), [49])

    @HazelcastTestCase.skip('Paging predicate with equal values will be supported in Hazelcast 4.0')
    def _test_equal_values_paging(self):
        self._fill_map_numeric()

        # keys[50 - 99], values[0 - 49]:
        for i in range(50, 100):
            self.map.put(i, i - 50)

        paging = PagingPredicate(is_less_than_or_equal_to('this', 8), 5)
        assertCountEqual(self, list(self.map.values(paging)), [0, 0, 1, 1, 2])
        paging.next_page()
        assertCountEqual(self, list(self.map.values(paging)), [2, 3, 3, 4, 4])
        paging.next_page()
        assertCountEqual(self, list(self.map.values(paging)), [5, 5, 6, 6, 7])
        paging.next_page()
        assertCountEqual(self, list(self.map.values(paging)), [7, 8, 8])

    # TODO: Add custom comparator tests!!!
    # case for values of custom class objects that are not "comparable" and no comparator provided.

    # TODO: Add serialization tests?

    def _fill_map(self):
        self.map.put('key-1', 1)
        self.map.put('key-2', 2)
        self.map.put('key-3', 3)

    def _fill_map_numeric(self, count=50):
        for n in range(0, count):
            self.map.put(n, n)

    def _configure_cluster(self):
        current_directory = os.path.dirname(__file__)
        with open(get_abs_path(current_directory, "hazelcast.xml"), "r") as f:
            return f.read()
