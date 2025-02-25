import os

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
from tests.integration.backward_compatible.util import (
    write_string_to_writer,
    read_string_from_reader,
)
from tests.util import random_string, get_abs_path
from hazelcast import HazelcastClient


class PredicateTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    def setUp(self):
        self.map = self.client.get_map(random_string()).blocking()

    def tearDown(self):
        self.map.destroy()

    def fill_map(self, count=10):
        m = {"key-%d" % x: "value-%d" % x for x in range(0, count)}
        self.map.put_all(m)
        return m

    def fill_map_numeric(self, count=100):
        m = {n: n for n in range(count)}
        self.map.put_all(m)

    def test_key_set(self):
        self.fill_map()
        key_set = self.map.key_set()
        list(key_set)
        key_set_list = list(key_set)
        assert key_set_list[0]

    def test_sql(self):
        self.fill_map()
        predicate = sql("this == 'value-1'")
        self.assertCountEqual(self.map.key_set(predicate), ["key-1"])

    def test_and(self):
        self.fill_map()
        predicate = and_(equal("this", "value-1"), equal("this", "value-2"))
        self.assertCountEqual(self.map.key_set(predicate), [])

    def test_or(self):
        self.fill_map()
        predicate = or_(equal("this", "value-1"), equal("this", "value-2"))
        self.assertCountEqual(self.map.key_set(predicate), ["key-1", "key-2"])

    def test_not(self):
        self.fill_map(count=3)
        predicate = not_(equal("this", "value-1"))
        self.assertCountEqual(self.map.key_set(predicate), ["key-0", "key-2"])

    def test_between(self):
        self.fill_map_numeric()

        predicate = between("this", 1, 20)
        self.assertCountEqual(self.map.key_set(predicate), list(range(1, 21)))

    def test_equal(self):
        self.fill_map()
        predicate = equal("this", "value-1")
        self.assertCountEqual(self.map.key_set(predicate), ["key-1"])

    def test_not_equal(self):
        self.fill_map(count=3)
        predicate = not_equal("this", "value-1")

        self.assertCountEqual(self.map.key_set(predicate), ["key-0", "key-2"])

    def test_in(self):
        self.fill_map_numeric(count=10)
        predicate = in_("this", 1, 5, 7)

        self.assertCountEqual(self.map.key_set(predicate), [1, 5, 7])

    def test_less_than(self):
        self.fill_map_numeric()
        predicate = less("this", 10)
        self.assertCountEqual(self.map.key_set(predicate), list(range(0, 10)))

    def test_less_than_or_equal(self):
        self.fill_map_numeric()
        predicate = less_or_equal("this", 10)
        self.assertCountEqual(self.map.key_set(predicate), list(range(0, 11)))

    def test_greater_than(self):
        self.fill_map_numeric()
        predicate = greater("this", 10)
        self.assertCountEqual(self.map.key_set(predicate), list(range(11, 100)))

    def test_greater_than_or_equal(self):
        self.fill_map_numeric()
        predicate = greater_or_equal("this", 10)
        self.assertCountEqual(self.map.key_set(predicate), list(range(10, 100)))

    def test_like(self):
        self.map.put("key-1", "a_value")
        self.map.put("key-2", "b_value")
        self.map.put("key-3", "aa_value")
        self.map.put("key-4", "AA_value")

        predicate = like("this", "a%")

        self.assertCountEqual(self.map.key_set(predicate), ["key-1", "key-3"])

    def test_ilike(self):
        self.map.put("key-1", "a_value")
        self.map.put("key-2", "b_value")
        self.map.put("key-3", "AA_value")

        predicate = ilike("this", "a%")

        self.assertCountEqual(self.map.key_set(predicate), ["key-1", "key-3"])

    def test_regex(self):
        self.map.put("key-1", "car")
        self.map.put("key-2", "cry")
        self.map.put("key-3", "giraffe")

        predicate = regex("this", "c[ar].*")

        self.assertCountEqual(self.map.key_set(predicate), ["key-1", "key-2"])

    def test_instance_of(self):
        self.map.put("key-1", True)
        self.map.put("key-2", 5)
        self.map.put("key-3", "str")

        predicate = instance_of("java.lang.Boolean")

        self.assertCountEqual(self.map.key_set(predicate), ["key-1"])

    def test_true(self):
        m = self.fill_map()
        predicate = true()
        self.assertCountEqual(self.map.key_set(predicate), list(m.keys()))

    def test_false(self):
        self.fill_map()
        predicate = false()
        self.assertCountEqual(self.map.key_set(predicate), [])

    def test_paging(self):
        self.fill_map_numeric()
        predicate = paging(less("this", 4), 2)
        self.assertCountEqual([0, 1], self.map.key_set(predicate))
        predicate.next_page()
        self.assertCountEqual([2, 3], self.map.key_set(predicate))
        predicate.next_page()
        self.assertCountEqual([], self.map.key_set(predicate))


class SimplePortable(Portable):
    def __init__(self, field=None):
        self.field = field

    def write_portable(self, writer):
        writer.write_int("field", self.field)

    def read_portable(self, reader):
        self.field = reader.read_int("field")

    def get_factory_id(self):
        return 1

    def get_class_id(self):
        return 1


class PredicatePortableTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        config["portable_factories"] = {1: {1: SimplePortable}}
        return config

    def setUp(self):
        self.map = self.client.get_map(random_string()).blocking()

    def tearDown(self):
        self.map.destroy()

    def fill_map(self, count=1000):
        m = {x: SimplePortable(x) for x in range(0, count)}
        self.map.put_all(m)
        return m

    def test_predicate_portable_key(self):
        _map = self.fill_map()
        map_keys = list(_map.keys())

        predicate = sql("field >= 900")
        entries = self.map.entry_set(predicate)
        self.assertEqual(len(entries), 100)
        for k, v in entries:
            self.assertGreaterEqual(v.field, 900)
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
            write_string_to_writer(writer, "name", self.name)
            writer.write_portable("limb", self.limb)

        def read_portable(self, reader):
            self.name = read_string_from_reader(reader, "name")
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
            write_string_to_writer(writer, "name", self.name)

        def read_portable(self, reader):
            self.name = read_string_from_reader(reader, "name")

        def __eq__(self, other):
            return isinstance(other, self.__class__) and self.name == other.name

    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        config["portable_factories"] = {
            1: {
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
    rc = None
    cluster = None
    client = None
    map = None

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
