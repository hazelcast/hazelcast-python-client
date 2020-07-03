from hazelcast.proxy import MAP_SERVICE
from hazelcast.proxy import aggregators
from hazelcast.serialization import predicate
from hazelcast.proxy import map
from tests.base import SingleMemberTestCase
from hazelcast import six


class MapAggregatorTest(SingleMemberTestCase):

    def _fill_map(self, count, map):
        for n in range(0, count):
            map.put(n, n)
        return map

    def _fill_map_same(self, count, map, num):
        for n in range(0, count):
            map.put(num, num)
        return map

    def test_count(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(50, test_map.aggregate(aggregators.count()))
        test_map.destroy()

    def test_count_with_attribute_path(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(50, test_map.aggregate(aggregators.count(attribute_path="this")))
        test_map.destroy()

    def test_count_with_predicate(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(47, test_map.aggregate(aggregators.count(), predicate.is_greater_than("this", 2)))
        test_map.destroy()

    def test_float_average(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(24.5, test_map.aggregate(aggregators.float_avg()))
        test_map.destroy()

    def test_float_average_with_attribute_path(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(24.5, test_map.aggregate(aggregators.float_avg(attribute_path="this")))
        test_map.destroy()

    def test_float_average_with_predicate(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(48.5, test_map.aggregate(aggregators.float_avg(),predicate.is_greater_than("this",47)))
        test_map.destroy()

    def test_float_sum(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(1225, test_map.aggregate(aggregators.float_sum()))
        test_map.destroy()

    def test_float_sum_with_attribute_path(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(1225, test_map.aggregate(aggregators.float_sum(attribute_path="this")))
        test_map.destroy()

    def test_float_sum_with_predicate(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(144, test_map.aggregate(aggregators.float_sum(), predicate.is_greater_than("this",46)))
        test_map.destroy()

    def test_average(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(24.5, test_map.aggregate(aggregators.average()))
        test_map.destroy()

    def test_average_with_attribute_path(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(24.5, test_map.aggregate(aggregators.average(attribute_path="this")))
        test_map.destroy()

    def test_average_with_predicate(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(48.5, test_map.aggregate(aggregators.average(), predicate.is_greater_than("this", 47)))
        test_map.destroy()

    def test_max(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(49, test_map.aggregate(aggregators.max()))
        test_map.destroy()

    def test_max_with_attribute_path(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(49, test_map.aggregate(aggregators.max(attribute_path="this")))
        test_map.destroy()

    def test_max_with_predicate(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(3, test_map.aggregate(aggregators.max(), predicate.is_less_than("this", 4)))
        test_map.destroy()

    def test_min(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(0, test_map.aggregate(aggregators.min()))
        test_map.destroy()

    def test_min_with_attribute_path(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(0, test_map.aggregate(aggregators.min(attribute_path="this")))
        test_map.destroy()

    def test_min_with_predicate(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(1, test_map.aggregate(aggregators.min(), predicate.is_not_equal_to("this", 0)))
        test_map.destroy()

    def test_sum(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(1225, test_map.aggregate(aggregators.sum()))
        test_map.destroy()

    def test_sum_with_attribute_path(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(1225, test_map.aggregate(aggregators.sum(attribute_path="this")))
        test_map.destroy()

    def test_sum_with_predicate(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(97, test_map.aggregate(aggregators.sum(), predicate.is_greater_than("this", 47)))
        test_map.destroy()

    def test_fixed_point_sum(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(1225, test_map.aggregate(aggregators.fixed_point_sum()))
        test_map.destroy()

    def test_fixed_point_sum_with_attribute_path(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(1225, test_map.aggregate(aggregators.fixed_point_sum(attribute_path="this")))
        test_map.destroy()

    def test_fixed_point_sum_with_predicate(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(144, test_map.aggregate(aggregators.fixed_point_sum(), predicate.is_greater_than("this", 46)))
        test_map.destroy()

    def test_floating_point_sum(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(1225, test_map.aggregate(aggregators.floating_point_sum()))
        test_map.destroy()

    def test_floating_point_sum_with_attribute_path(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(1225, test_map.aggregate(aggregators.floating_point_sum(attribute_path="this")))
        test_map.destroy()

    def test_floating_point_sum_with_predicate(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(144, test_map.aggregate(aggregators.floating_point_sum(), predicate.is_greater_than("this", 46)))
        test_map.destroy()

    def test_distinct_values(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map_same(50, test_map, 1)
        self.assertEqual([1, 1], test_map.aggregate(aggregators.distinct_values()))
        test_map.destroy()

    def test_distinct_values_with_attribute_path(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(test_map, test_map.aggregate(aggregators.distinct_values(attribute_path="this")))
        test_map.destroy()

    def test_distinct_values_with_predicate(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(test_map, test_map.aggregate(aggregators.distinct_values(), predicate.is_less_than("this", 50)))
        test_map.destroy()

    def test_max_by(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(49, test_map.aggregate(aggregators.max_by()))
        test_map.destroy()

    def test_max_by_with_attribute_path(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(49, test_map.aggregate(aggregators.max_by(attribute_path="this")))
        test_map.destroy()

    def test_max_by_with_predicate(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(3, test_map.aggregate(aggregators.max_by(), predicate.is_less_than("this", 4)))
        test_map.destroy()

    def test_min_by(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(0, test_map.aggregate(aggregators.min_by()))
        test_map.destroy()

    def test_min_by_with_attribute_path(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(0, test_map.aggregate(aggregators.min_by(attribute_path="this")))
        test_map.destroy()

    def test_min_by_with_predicate(self):
        test_map = self.client.get_map("test_map").blocking()
        self._fill_map(50, test_map)
        self.assertEqual(48, test_map.aggregate(aggregators.min_by(), predicate.is_greater_than("this", 47)))
        test_map.destroy()

#json string hz jsonvalue
#distinct check


