import os
from unittest import TestCase

from hazelcast.predicate import (
    equal,
    and_,
    between,
    less,
    less_or_equal,
    greater,
    greater_or_equal,
    or_,
    not_equal,
    not_,
    like,
    ilike,
    regex,
    sql,
    true,
    false,
    in_,
    instance_of,
    paging,
)
from hazelcast.serialization.api import Portable, IdentifiedDataSerializable
from hazelcast.util import IterationType
from tests.base import SingleMemberTestCase, HazelcastTestCase
from tests.serialization.portable_test import InnerPortable, FACTORY_ID
from tests.util import random_string, get_abs_path
from hazelcast import six, HazelcastClient
from hazelcast.six.moves import range


class PredicateStrTest(TestCase):
    def test_sql(self):
        predicate = sql("this == 'value-1'")
        self.assertEqual(str(predicate), "SqlPredicate(sql='this == 'value-1'')")

    def test_and(self):
        predicate = and_(equal("this", "value-1"), equal("this", "value-2"))
        self.assertEqual(
            str(predicate),
            "AndPredicate(EqualPredicate(attribute='this', value=value-1),"
            " EqualPredicate(attribute='this', value=value-2))",
        )

    def test_between(self):
        predicate = between("this", 1, 20)
        self.assertEqual(str(predicate), "BetweenPredicate(attribute='this', from=1, to=20)")

    def test_equal_str(self):
        predicate = equal("this", "value-1")
        self.assertEqual(str(predicate), "EqualPredicate(attribute='this', value=value-1)")

    def test_greater_less(self):
        predicate = less_or_equal("this", 10)
        self.assertEqual(
            str(predicate),
            "GreaterLessPredicate(attribute='this', value=10, is_equal=True, is_less=True)",
        )

    def test_like(self):
        predicate = like("this", "a%")
        self.assertEqual(str(predicate), "LikePredicate(attribute='this', expression='a%')")

    def test_ilike(self):
        predicate = ilike("this", "a%")
        self.assertEqual(str(predicate), "ILikePredicate(attribute='this', expression='a%')")

    def test_in(self):
        predicate = in_("this", 1, 5, 7)
        self.assertEqual(str(predicate), "InPredicate(attribute='this', 1,5,7)")

    def test_instance_of(self):
        predicate = instance_of("java.lang.Boolean")
        self.assertEqual(str(predicate), "InstanceOfPredicate(class_name='java.lang.Boolean')")

    def test_not_equal(self):
        predicate = not_equal("this", "value-1")
        self.assertEqual(str(predicate), "NotEqualPredicate(attribute='this', value=value-1)")

    def test_not(self):
        predicate = not_(equal("this", "value-1"))
        self.assertEqual(
            str(predicate),
            "NotPredicate(predicate=EqualPredicate(attribute='this', value=value-1))",
        )

    def test_or(self):
        predicate = or_(equal("this", "value-1"), equal("this", "value-2"))
        self.assertEqual(
            str(predicate),
            "OrPredicate(EqualPredicate(attribute='this', value=value-1),"
            " EqualPredicate(attribute='this', value=value-2))",
        )

    def test_regex(self):
        predicate = regex("this", "c[ar].*")
        self.assertEqual(str(predicate), "RegexPredicate(attribute='this', pattern='c[ar].*')")

    def test_true(self):
        predicate = true()
        self.assertEqual(str(predicate), "TruePredicate()")

    def test_false(self):
        predicate = false()
        self.assertEqual(str(predicate), "FalsePredicate()")

    def test_paging(self):
        predicate = paging(true(), 5)
        self.assertEqual(
            str(predicate),
            "PagingPredicate(predicate=TruePredicate(), page_size=5, comparator=None)",
        )


class PredicateTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    def setUp(self):
        self.map = self.client.get_map(random_string()).blocking()

    def tearDown(self):
        self.map.destroy()

    def _fill_map(self, count=10):
        m = {"key-%d" % x: "value-%d" % x for x in range(0, count)}
        self.map.put_all(m)
        return m

    def _fill_map_numeric(self, count=100):
        m = {n: n for n in range(count)}
        self.map.put_all(m)

    def test_key_set(self):
        self._fill_map()
        key_set = self.map.key_set()
        list(key_set)
        key_set_list = list(key_set)
        assert key_set_list[0]

    def test_sql(self):
        self._fill_map()
        predicate = sql("this == 'value-1'")
        six.assertCountEqual(self, self.map.key_set(predicate), ["key-1"])

    def test_and(self):
        self._fill_map()
        predicate = and_(equal("this", "value-1"), equal("this", "value-2"))
        six.assertCountEqual(self, self.map.key_set(predicate), [])

    def test_or(self):
        self._fill_map()
        predicate = or_(equal("this", "value-1"), equal("this", "value-2"))
        six.assertCountEqual(self, self.map.key_set(predicate), ["key-1", "key-2"])

    def test_not(self):
        self._fill_map(count=3)
        predicate = not_(equal("this", "value-1"))
        six.assertCountEqual(self, self.map.key_set(predicate), ["key-0", "key-2"])

    def test_between(self):
        self._fill_map_numeric()

        predicate = between("this", 1, 20)
        six.assertCountEqual(self, self.map.key_set(predicate), list(range(1, 21)))

    def test_equal(self):
        self._fill_map()
        predicate = equal("this", "value-1")
        six.assertCountEqual(self, self.map.key_set(predicate), ["key-1"])

    def test_not_equal(self):
        self._fill_map(count=3)
        predicate = not_equal("this", "value-1")

        six.assertCountEqual(self, self.map.key_set(predicate), ["key-0", "key-2"])

    def test_in(self):
        self._fill_map_numeric(count=10)
        predicate = in_("this", 1, 5, 7)

        six.assertCountEqual(self, self.map.key_set(predicate), [1, 5, 7])

    def test_less_than(self):
        self._fill_map_numeric()
        predicate = less("this", 10)
        six.assertCountEqual(self, self.map.key_set(predicate), list(range(0, 10)))

    def test_less_than_or_equal(self):
        self._fill_map_numeric()
        predicate = less_or_equal("this", 10)
        six.assertCountEqual(self, self.map.key_set(predicate), list(range(0, 11)))

    def test_greater_than(self):
        self._fill_map_numeric()
        predicate = greater("this", 10)
        six.assertCountEqual(self, self.map.key_set(predicate), list(range(11, 100)))

    def test_greater_than_or_equal(self):
        self._fill_map_numeric()
        predicate = greater_or_equal("this", 10)
        six.assertCountEqual(self, self.map.key_set(predicate), list(range(10, 100)))

    def test_like(self):
        self.map.put("key-1", "a_value")
        self.map.put("key-2", "b_value")
        self.map.put("key-3", "aa_value")
        self.map.put("key-4", "AA_value")

        predicate = like("this", "a%")

        six.assertCountEqual(self, self.map.key_set(predicate), ["key-1", "key-3"])

    def test_ilike(self):
        self.map.put("key-1", "a_value")
        self.map.put("key-2", "b_value")
        self.map.put("key-3", "AA_value")

        predicate = ilike("this", "a%")

        six.assertCountEqual(self, self.map.key_set(predicate), ["key-1", "key-3"])

    def test_regex(self):
        self.map.put("key-1", "car")
        self.map.put("key-2", "cry")
        self.map.put("key-3", "giraffe")

        predicate = regex("this", "c[ar].*")

        six.assertCountEqual(self, self.map.key_set(predicate), ["key-1", "key-2"])

    def test_instance_of(self):
        self.map.put("key-1", True)
        self.map.put("key-2", 5)
        self.map.put("key-3", "str")

        predicate = instance_of("java.lang.Boolean")

        six.assertCountEqual(self, self.map.key_set(predicate), ["key-1"])

    def test_true(self):
        m = self._fill_map()
        predicate = true()
        six.assertCountEqual(self, self.map.key_set(predicate), list(m.keys()))

    def test_false(self):
        self._fill_map()
        predicate = false()
        six.assertCountEqual(self, self.map.key_set(predicate), [])

    def test_paging(self):
        self._fill_map_numeric()
        predicate = paging(less("this", 4), 2)
        six.assertCountEqual(self, [0, 1], self.map.key_set(predicate))
        predicate.next_page()
        six.assertCountEqual(self, [2, 3], self.map.key_set(predicate))
        predicate.next_page()
        six.assertCountEqual(self, [], self.map.key_set(predicate))


class PredicatePortableTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        config["portable_factories"] = {FACTORY_ID: {InnerPortable.CLASS_ID: InnerPortable}}
        return config

    def setUp(self):
        self.map = self.client.get_map(random_string()).blocking()

    def tearDown(self):
        self.map.destroy()

    def _fill_map(self, count=1000):
        m = {
            InnerPortable("key-%d" % x, x): InnerPortable("value-%d" % x, x)
            for x in range(0, count)
        }
        self.map.put_all(m)
        return m

    def test_predicate_portable_key(self):
        _map = self._fill_map()
        map_keys = list(_map.keys())

        predicate = sql("param_int >= 900")
        key_set = self.map.key_set(predicate)
        self.assertEqual(len(key_set), 100)
        for k in key_set:
            self.assertGreaterEqual(k.param_int, 900)
            self.assertIn(k, map_keys)


class NestedPredicatePortableTest(SingleMemberTestCase):
    class Body(Portable):
        def __init__(self, name=None, limb=None):
            self.name = name
            self.limb = limb

        def get_class_id(self):
            return 1

        def get_factory_id(self):
            return 1

        def get_class_version(self):
            return 15

        def write_portable(self, writer):
            writer.write_string("name", self.name)
            writer.write_portable("limb", self.limb)

        def read_portable(self, reader):
            self.name = reader.read_string("name")
            self.limb = reader.read_portable("limb")

        def __eq__(self, other):
            return isinstance(other, self.__class__) and (self.name, self.limb) == (
                other.name,
                other.limb,
            )

    class Limb(Portable):
        def __init__(self, name=None):
            self.name = name

        def get_class_id(self):
            return 2

        def get_factory_id(self):
            return 1

        def get_class_version(self):
            return 2

        def write_portable(self, writer):
            writer.write_string("name", self.name)

        def read_portable(self, reader):
            self.name = reader.read_string("name")

        def __eq__(self, other):
            return isinstance(other, self.__class__) and self.name == other.name

    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        config["portable_factories"] = {
            FACTORY_ID: {
                1: NestedPredicatePortableTest.Body,
                2: NestedPredicatePortableTest.Limb,
            },
        }
        return config

    def setUp(self):
        self.map = self.client.get_map(random_string()).blocking()
        self.map.put(
            1, NestedPredicatePortableTest.Body("body1", NestedPredicatePortableTest.Limb("hand"))
        )
        self.map.put(
            2, NestedPredicatePortableTest.Body("body2", NestedPredicatePortableTest.Limb("leg"))
        )

    def tearDown(self):
        self.map.destroy()

    def test_adding_indexes(self):
        # single-attribute index
        self.map.add_index(attributes=["name"])

        # nested-attribute index
        self.map.add_index(attributes=["limb.name"])

    def test_single_attribute_query_portable_predicates(self):
        predicate = equal("limb.name", "hand")
        values = self.map.values(predicate)

        self.assertEqual(1, len(values))
        self.assertEqual("body1", values[0].name)

    def test_nested_attribute_query_sql_predicate(self):
        predicate = sql("limb.name == 'leg'")
        values = self.map.values(predicate)

        self.assertEqual(1, len(values))
        self.assertEqual("body2", values[0].name)


class PagingPredicateTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc, cls.configure_cluster())
        cls.cluster.start_member()
        cls.cluster.start_member()
        cls.client = HazelcastClient(cluster_name=cls.cluster.id)
        cls.map = cls.client.get_map(random_string()).blocking()

    def setUp(self):
        self.map.clear()

    @classmethod
    def tearDownClass(cls):
        cls.map.destroy()
        cls.client.shutdown()
        cls.rc.shutdownCluster(cls.cluster.id)
        cls.rc.exit()

    @staticmethod
    def configure_cluster():
        current_directory = os.path.dirname(__file__)
        with open(
            get_abs_path(os.path.join(current_directory, "proxy"), "hazelcast.xml"), "r"
        ) as f:
            return f.read()

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

    def test_entry_set_with_paging_predicate(self):
        self.fill_map(3)
        entry_set = self.map.entry_set(paging(greater_or_equal("this", 2), 1))
        self.assertEqual(len(entry_set), 1)
        self.assertEqual(entry_set[0], ("key-2", 2))

    def test_key_set_with_paging_predicate(self):
        self.fill_map(3)
        key_set = self.map.key_set(paging(greater_or_equal("this", 2), 1))
        self.assertEqual(len(key_set), 1)
        self.assertEqual(key_set[0], "key-2")

    def test_values_with_paging_predicate(self):
        self.fill_map(3)
        values = self.map.values(paging(greater_or_equal("this", 2), 1))
        self.assertEqual(len(values), 1)
        self.assertEqual(values[0], 2)

    def test_with_none_inner_predicate(self):
        self.fill_map(3)
        predicate = paging(None, 10)
        self.assertEqual(self.map.values(predicate), [0, 1, 2])

    def test_first_page(self):
        self.fill_map()
        predicate = paging(greater_or_equal("this", 40), 2)
        self.assertEqual(self.map.values(predicate), [40, 41])

    def test_next_page(self):
        self.fill_map()
        predicate = paging(greater_or_equal("this", 40), 2)
        predicate.next_page()
        self.assertEqual(self.map.values(predicate), [42, 43])

    def test_set_page(self):
        self.fill_map()
        predicate = paging(greater_or_equal("this", 40), 2)
        predicate.page = 4
        self.assertEqual(self.map.values(predicate), [48, 49])

    def test_get_page(self):
        predicate = paging(greater_or_equal("this", 40), 2)
        predicate.page = 4
        self.assertEqual(predicate.page, 4)

    def test_page_size(self):
        predicate = paging(greater_or_equal("this", 40), 2)
        self.assertEqual(predicate.page_size, 2)

    def test_previous_page(self):
        self.fill_map()
        predicate = paging(greater_or_equal("this", 40), 2)
        predicate.page = 4
        predicate.previous_page()
        self.assertEqual(self.map.values(predicate), [46, 47])

    def test_get_4th_then_previous_page(self):
        self.fill_map()
        predicate = paging(greater_or_equal("this", 40), 2)
        predicate.page = 4
        self.map.values(predicate)
        predicate.previous_page()
        self.assertEqual(self.map.values(predicate), [46, 47])

    def test_get_3rd_then_next_page(self):
        self.fill_map()
        predicate = paging(greater_or_equal("this", 40), 2)
        predicate.page = 3
        self.map.values(predicate)
        predicate.next_page()
        self.assertEqual(self.map.values(predicate), [48, 49])

    def test_set_nonexistent_page(self):
        # Trying to get page 10, which is out of range, should return empty list.
        self.fill_map()
        predicate = paging(greater_or_equal("this", 40), 2)
        predicate.page = 10
        self.assertEqual(self.map.values(predicate), [])

    def test_nonexistent_previous_page(self):
        # Trying to get previous page while already at first page should return first page.
        self.fill_map()
        predicate = paging(greater_or_equal("this", 40), 2)
        predicate.previous_page()
        self.assertEqual(self.map.values(predicate), [40, 41])

    def test_nonexistent_next_page(self):
        # Trying to get next page while already at last page should return empty list.
        self.fill_map()
        predicate = paging(greater_or_equal("this", 40), 2)
        predicate.page = 4
        predicate.next_page()
        self.assertEqual(self.map.values(predicate), [])

    def test_get_half_full_last_page(self):
        # Page size set to 2, but last page only has 1 element.
        self.fill_map()
        predicate = paging(greater_or_equal("this", 41), 2)
        predicate.page = 4
        self.assertEqual(self.map.values(predicate), [49])

    def test_reset(self):
        self.fill_map()
        predicate = paging(greater_or_equal("this", 40), 2)
        self.assertEqual(self.map.values(predicate), [40, 41])
        predicate.next_page()
        self.assertEqual(self.map.values(predicate), [42, 43])
        predicate.reset()
        self.assertEqual(self.map.values(predicate), [40, 41])

    def test_empty_map(self):
        # Empty map should return empty list.
        predicate = paging(greater_or_equal("this", 30), 2)
        self.assertEqual(self.map.values(predicate), [])

    def test_equal_values_paging(self):
        self.fill_map()
        # keys[50 - 99], values[0 - 49]:
        m = {"key-%d" % i: i - 50 for i in range(50, 100)}
        self.map.put_all(m)

        predicate = paging(less_or_equal("this", 8), 5)

        self.assertEqual(self.map.values(predicate), [0, 0, 1, 1, 2])
        predicate.next_page()
        self.assertEqual(self.map.values(predicate), [2, 3, 3, 4, 4])
        predicate.next_page()
        self.assertEqual(self.map.values(predicate), [5, 5, 6, 6, 7])
        predicate.next_page()
        self.assertEqual(self.map.values(predicate), [7, 8, 8])

    def test_entry_set_with_custom_comparator(self):
        m = self.fill_map()
        predicate = paging(less("this", 10), 5, CustomComparator(1, IterationType.KEY))

        def entries(start, end):
            return list(
                sorted(
                    map(lambda k: (k, m[k]), filter(lambda k: start <= m[k] < end, m)),
                    key=lambda e: e[1],
                    reverse=True,
                )
            )

        self.assertEqual(entries(5, 10), self.map.entry_set(predicate))
        predicate.next_page()
        self.assertEqual(entries(0, 5), self.map.entry_set(predicate))
        predicate.next_page()
        self.assertEqual([], self.map.entry_set(predicate))

    def test_key_set_with_custom_comparator(self):
        m = self.fill_map()
        predicate = paging(less("this", 10), 5, CustomComparator(1, IterationType.KEY))

        keys = list(sorted(m.keys(), key=lambda k: m[k]))

        self.assertEqual(keys[9:4:-1], self.map.key_set(predicate))
        predicate.next_page()
        self.assertEqual(keys[4::-1], self.map.key_set(predicate))
        predicate.next_page()
        self.assertEqual([], self.map.key_set(predicate))

    def test_values_with_custom_comparator(self):
        m = self.fill_map()
        predicate = paging(less("this", 10), 5, CustomComparator(1, IterationType.KEY))

        values = list(sorted(m.values()))

        self.assertEqual(values[9:4:-1], self.map.values(predicate))
        predicate.next_page()
        self.assertEqual(values[4::-1], self.map.values(predicate))
        predicate.next_page()
        self.assertEqual([], self.map.values(predicate))

    def fill_map(self, count=50):
        m = {"key-%d" % x: x for x in range(count)}
        self.map.put_all(m)
        return m


class CustomComparator(IdentifiedDataSerializable):
    """
    For type:

    - 0 -> lexicographical order
    - 1 -> reverse lexicographical
    - 2 -> length increasing order

    Iteration type is same as the ``hazelcast.util.IterationType``
    """

    def __init__(self, order, iteration_type):
        self.order = order
        self.iteration_type = iteration_type

    def write_data(self, object_data_output):
        object_data_output.write_int(self.order)
        object_data_output.write_int(self.iteration_type)

    def read_data(self, object_data_input):
        pass

    def get_factory_id(self):
        return 66

    def get_class_id(self):
        return 2
