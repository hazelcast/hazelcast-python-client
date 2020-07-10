from hazelcast.core import HazelcastJsonValue
from hazelcast.proxy import MAP_SERVICE
from hazelcast.proxy import aggregators
from hazelcast.serialization import predicate
from hazelcast.proxy import map
from tests.base import SingleMemberTestCase
from hazelcast import six


class MapAggregatorTest(SingleMemberTestCase):

    def setUp(self):
        self.map = self.client.get_map("test-map").blocking()

    def tearDown(self):
        self.map.destroy()

    def _fill_map(self, count, map):
        for n in range(0, count):
            map.put(n, n)
        return map

    def _fill_map_same(self, count, map, num):
        for n in range(0, count):
            map.put(num, num)
        return map

    def test_count(self):
        self._fill_map(50, self.map)
        self.assertEqual(50, self.map.aggregate(aggregators.count()))

    def test_count_with_attribute_path(self):
        self._fill_map(50, self.map)
        self.assertEqual(50, self.map.aggregate(aggregators.count(attribute_path="this")))

    def test_count_with_predicate(self):
        self._fill_map(50, self.map)
        self.assertEqual(47, self.map.aggregate(aggregators.count(), predicate.is_greater_than("this", 2)))

    def test_float_average(self):
        self._fill_map(50, self.map)
        self.assertEqual(24.5, self.map.aggregate(aggregators.float_avg()))

    def test_float_average_with_attribute_path(self):
        self._fill_map(50, self.map)
        self.assertEqual(24.5, self.map.aggregate(aggregators.float_avg(attribute_path="this")))

    def test_float_average_with_predicate(self):
        self._fill_map(50, self.map)
        self.assertEqual(48.5, self.map.aggregate(aggregators.float_avg(),predicate.is_greater_than("this",47)))

    def test_float_sum(self):
        self._fill_map(50, self.map)
        self.assertEqual(1225, self.map.aggregate(aggregators.float_sum()))

    def test_float_sum_with_attribute_path(self):
        self._fill_map(50, self.map)
        self.assertEqual(1225, self.map.aggregate(aggregators.float_sum(attribute_path="this")))

    def test_float_sum_with_predicate(self):
        self._fill_map(50, self.map)
        self.assertEqual(144, self.map.aggregate(aggregators.float_sum(), predicate.is_greater_than("this",46)))

    def test_average(self):
        self._fill_map(50, self.map)
        self.assertEqual(24.5, self.map.aggregate(aggregators.average()))

    def test_average_with_attribute_path(self):
        self._fill_map(50, self.map)
        self.assertEqual(24.5, self.map.aggregate(aggregators.average(attribute_path="this")))

    def test_average_with_predicate(self):
        self._fill_map(50, self.map)
        self.assertEqual(48.5, self.map.aggregate(aggregators.average(), predicate.is_greater_than("this", 47)))

    def test_max(self):
        self._fill_map(50, self.map)
        self.assertEqual(49, self.map.aggregate(aggregators.max()))

    def test_max_with_attribute_path(self):
        self._fill_map(50, self.map)
        self.assertEqual(49, self.map.aggregate(aggregators.max(attribute_path="this")))

    def test_max_with_predicate(self):
        self._fill_map(50, self.map)
        self.assertEqual(3, self.map.aggregate(aggregators.max(), predicate.is_less_than("this", 4)))

    def test_min(self):
        self._fill_map(50, self.map)
        self.assertEqual(0, self.map.aggregate(aggregators.min()))

    def test_min_with_attribute_path(self):
        self._fill_map(50, self.map)
        self.assertEqual(0, self.map.aggregate(aggregators.min(attribute_path="this")))

    def test_min_with_predicate(self):
        self._fill_map(50, self.map)
        self.assertEqual(27, self.map.aggregate(aggregators.min(), predicate.is_greater_than("this", 26)))

    def test_sum(self):
        self._fill_map(50, self.map)
        self.assertEqual(1225, self.map.aggregate(aggregators.sum()))

    def test_sum_with_attribute_path(self):
        self._fill_map(50, self.map)
        self.assertEqual(1225, self.map.aggregate(aggregators.sum(attribute_path="this")))

    def test_sum_with_predicate(self):
        self._fill_map(50, self.map)
        self.assertEqual(97, self.map.aggregate(aggregators.sum(), predicate.is_greater_than("this", 47)))

    def test_fixed_point_sum(self):
        self._fill_map(50, self.map)
        self.assertEqual(1225, self.map.aggregate(aggregators.fixed_point_sum()))

    def test_fixed_point_sum_with_attribute_path(self):
        self._fill_map(50, self.map)
        self.assertEqual(1225, self.map.aggregate(aggregators.fixed_point_sum(attribute_path="this")))

    def test_fixed_point_sum_with_predicate(self):
        self._fill_map(50, self.map)
        self.assertEqual(144, self.map.aggregate(aggregators.fixed_point_sum(), predicate.is_greater_than("this", 46)))

    def test_floating_point_sum(self):
        self._fill_map(50, self.map)
        self.assertEqual(1225, self.map.aggregate(aggregators.floating_point_sum()))

    def test_floating_point_sum_with_attribute_path(self):
        self._fill_map(50, self.map)
        self.assertEqual(1225, self.map.aggregate(aggregators.floating_point_sum(attribute_path="this")))

    def test_floating_point_sum_with_predicate(self):
        self._fill_map(50, self.map)
        self.assertEqual(144, self.map.aggregate(aggregators.floating_point_sum(), predicate.is_greater_than("this", 46)))

    def test_distinct_values(self):
        self._fill_map_same(50, self.map, 1)
        six.assertCountEqual(self, [1], self.map.aggregate(aggregators.distinct_values()).values)

    def test_distinct_values_with_attribute_path(self):
        self._fill_map_same(50, self.map, 1)
        six.assertCountEqual(self, [1], self.map.aggregate(aggregators.distinct_values(attribute_path="this")).values)

    def test_distinct_values_with_predicate(self):
        self._fill_map_same(50, self.map,1)
        self.assertEqual(0, len(self.map.aggregate(aggregators.distinct_values(), predicate.is_greater_than("this", 2)).values))