from unittest import TestCase, skip

from hazelcast.serialization.predicate import is_equal_to, and_, is_between, is_less_than, \
    is_less_than_or_equal_to, is_greater_than, is_greater_than_or_equal_to, or_, is_not_equal_to, not_, is_like, \
    is_ilike, matches_regex, sql, true, false, is_in, is_instance_of, PagingPredicate
from hazelcast.serialization.api import Portable
from tests.base import SingleMemberTestCase
from tests.serialization.portable_test import InnerPortable, FACTORY_ID
from tests.util import random_string, set_attr
from hazelcast import six
from hazelcast.six.moves import range


class PredicateStrTest(TestCase):
    def test_sql(self):
        predicate = sql("this == 'value-1'")
        self.assertEqual(str(predicate), "SqlPredicate(sql='this == 'value-1'')")

    def test_and(self):
        predicate = and_(is_equal_to("this", "value-1"), is_equal_to("this", "value-2"))
        self.assertEqual(str(predicate), "AndPredicate(EqualPredicate(attribute='this', value=value-1),"
                                         " EqualPredicate(attribute='this', value=value-2))")

    def test_between(self):
        predicate = is_between("this", 1, 20)
        self.assertEqual(str(predicate), "BetweenPredicate(attribute='this', from=1, to=20)")

    def test_equal_str(self):
        predicate = is_equal_to("this", "value-1")
        self.assertEqual(str(predicate), "EqualPredicate(attribute='this', value=value-1)")

    def test_greater_less(self):
        predicate = is_less_than_or_equal_to("this", 10)
        self.assertEqual(str(predicate),
                         "GreaterLessPredicate(attribute='this', value=10, is_equal=True, is_less=True)")

    def test_like(self):
        predicate = is_like("this", "a%")
        self.assertEqual(str(predicate), "LikePredicate(attribute='this', expression='a%')")

    def test_ilike(self):
        predicate = is_ilike("this", "a%")
        self.assertEqual(str(predicate), "ILikePredicate(attribute='this', expression='a%')")

    def test_in(self):
        predicate = is_in("this", 1, 5, 7)
        self.assertEqual(str(predicate), "InPredicate(attribute='this', 1,5,7)")

    def test_instance_of(self):
        predicate = is_instance_of("java.lang.Boolean")
        self.assertEqual(str(predicate), "InstanceOfPredicate(class_name='java.lang.Boolean')")

    def test_not_equal(self):
        predicate = is_not_equal_to("this", "value-1")
        self.assertEqual(str(predicate), "NotEqualPredicate(attribute='this', value=value-1)")

    def test_not(self):
        predicate = not_(is_equal_to("this", "value-1"))
        self.assertEqual(str(predicate), "NotPredicate(predicate=EqualPredicate(attribute='this', value=value-1))")

    def test_or(self):
        predicate = or_(is_equal_to("this", "value-1"), is_equal_to("this", "value-2"))
        self.assertEqual(str(predicate), "OrPredicate(EqualPredicate(attribute='this', value=value-1),"
                                         " EqualPredicate(attribute='this', value=value-2))")

    def test_regex(self):
        predicate = matches_regex("this", "c[ar].*")
        self.assertEqual(str(predicate), "RegexPredicate(attribute='this', pattern='c[ar].*')")

    def test_true(self):
        predicate = true()
        self.assertEqual(str(predicate), "TruePredicate()")

    def test_false(self):
        predicate = false()
        self.assertEqual(str(predicate), "FalsePredicate()")

    def test_paging(self):
        # TODO: str test for custom comparator
        predicate = PagingPredicate(is_greater_than_or_equal_to('this', 10), 5)
        self.assertEqual(str(predicate), "PagingPredicate(predicate=GreaterLessPredicate(attribute='this', value=10,"
                                         " is_equal=True, is_less=False), page_size=5, comparator=None)")


class PredicateTest(SingleMemberTestCase):
    def setUp(self):
        self.map = self.client.get_map(random_string()).blocking()

    def tearDown(self):
        self.map.destroy()

    def _fill_map(self, count=10):
        map = {"key-%d" % x: "value-%d" % x for x in range(0, count)}
        for k, v in six.iteritems(map):
            self.map.put(k, v)
        return map

    def _fill_map_numeric(self, count=100):
        for n in range(0, count):
            self.map.put(n, n)

    def test_key_set(self):
        self._fill_map()
        key_set = self.map.key_set()
        key_set_list = list(key_set)
        key_set_list = list(key_set)
        assert key_set_list[0]

    def test_sql(self):
        self._fill_map()
        predicate = sql("this == 'value-1'")
        six.assertCountEqual(self, self.map.key_set(predicate), ["key-1"])

    def test_and(self):
        self._fill_map()
        predicate = and_(is_equal_to("this", "value-1"), is_equal_to("this", "value-2"))
        six.assertCountEqual(self, self.map.key_set(predicate), [])

    def test_or(self):
        self._fill_map()
        predicate = or_(is_equal_to("this", "value-1"), is_equal_to("this", "value-2"))
        six.assertCountEqual(self, self.map.key_set(predicate), ["key-1", "key-2"])

    def test_not(self):
        self._fill_map(count=3)
        predicate = not_(is_equal_to("this", "value-1"))
        six.assertCountEqual(self, self.map.key_set(predicate), ["key-0", "key-2"])

    def test_between(self):
        self._fill_map_numeric()

        predicate = is_between("this", 1, 20)
        six.assertCountEqual(self, self.map.key_set(predicate), list(range(1, 21)))

    def test_equal(self):
        self._fill_map()
        predicate = is_equal_to("this", "value-1")
        six.assertCountEqual(self, self.map.key_set(predicate), ["key-1"])

    def test_not_equal(self):
        self._fill_map(count=3)
        predicate = is_not_equal_to("this", "value-1")

        six.assertCountEqual(self, self.map.key_set(predicate), ["key-0", "key-2"])

    def test_in(self):
        self._fill_map_numeric(count=10)
        predicate = is_in("this", 1, 5, 7)

        six.assertCountEqual(self, self.map.key_set(predicate), [1, 5, 7])

    def test_less_than(self):
        self._fill_map_numeric()
        predicate = is_less_than("this", 10)
        six.assertCountEqual(self, self.map.key_set(predicate), list(range(0, 10)))

    def test_less_than_or_equal(self):
        self._fill_map_numeric()
        predicate = is_less_than_or_equal_to("this", 10)
        six.assertCountEqual(self, self.map.key_set(predicate), list(range(0, 11)))

    def test_greater_than(self):
        self._fill_map_numeric()
        predicate = is_greater_than("this", 10)
        six.assertCountEqual(self, self.map.key_set(predicate), list(range(11, 100)))

    def test_greater_than_or_equal(self):
        self._fill_map_numeric()
        predicate = is_greater_than_or_equal_to("this", 10)
        six.assertCountEqual(self, self.map.key_set(predicate), list(range(10, 100)))

    def test_like(self):
        self.map.put("key-1", "a_value")
        self.map.put("key-2", "b_value")
        self.map.put("key-3", "aa_value")
        self.map.put("key-4", "AA_value")

        predicate = is_like("this", "a%")

        six.assertCountEqual(self, self.map.key_set(predicate), ["key-1", "key-3"])

    def test_ilike(self):
        self.map.put("key-1", "a_value")
        self.map.put("key-2", "b_value")
        self.map.put("key-3", "AA_value")

        predicate = is_ilike("this", "a%")

        six.assertCountEqual(self, self.map.key_set(predicate), ["key-1", "key-3"])

    def test_regex(self):
        self.map.put("key-1", "car")
        self.map.put("key-2", "cry")
        self.map.put("key-3", "giraffe")

        predicate = matches_regex("this", "c[ar].*")

        six.assertCountEqual(self, self.map.key_set(predicate), ["key-1", "key-2"])

    @skip(reason="Default ClassLoader is null in SerializationService")
    def test_instance_of(self):
        self.map.put("key-1", True)
        self.map.put("key-2", 5)
        self.map.put("key-3", "str")

        predicate = is_instance_of("java.lang.Boolean")

        six.assertCountEqual(self, self.map.key_set(predicate), ["key-1"])

    def test_true(self):
        map = self._fill_map()

        predicate = true()

        six.assertCountEqual(self, self.map.key_set(predicate), list(map.keys()))

    def test_false(self):
        map = self._fill_map()

        predicate = false()

        six.assertCountEqual(self, self.map.key_set(predicate), [])


class PredicatePortableTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        the_factory = {InnerPortable.CLASS_ID: InnerPortable}
        config.serialization_config.portable_factories[FACTORY_ID] = the_factory
        return config

    def setUp(self):
        self.map = self.client.get_map(random_string()).blocking()

    def tearDown(self):
        self.map.destroy()

    def _fill_map(self, count=1000):
        map = {InnerPortable("key-%d" % x, x): InnerPortable("value-%d" % x, x) for x in range(0, count)}
        for k, v in six.iteritems(map):
            self.map.put(k, v)
        return map

    def test_predicate_portable_key(self):
        _map = self._fill_map()
        map_keys = list(_map.keys())

        predicate = sql("param_int >= 900")
        key_set = self.map.key_set(predicate)
        self.assertEqual(len(key_set), 100)
        for k in key_set:
            self.assertGreaterEqual(k.param_int, 900)
            self.assertIn(k, map_keys)


@set_attr(category=3.08)
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
            writer.write_utf("name", self.name)
            writer.write_portable("limb", self.limb)

        def read_portable(self, reader):
            self.name = reader.read_utf("name")
            self.limb = reader.read_portable("limb")

        def __eq__(self, other):
            return isinstance(other, self.__class__) and (self.name, self.limb) == (other.name, other.limb)

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
            writer.write_utf("name", self.name)

        def read_portable(self, reader):
            self.name = reader.read_utf("name")

        def __eq__(self, other):
            return isinstance(other, self.__class__) and self.name == other.name

    @classmethod
    def configure_client(cls, config):
        factory = {1: NestedPredicatePortableTest.Body, 2: NestedPredicatePortableTest.Limb}
        config.serialization_config.portable_factories[FACTORY_ID] = factory
        return config

    def setUp(self):
        self.map = self.client.get_map(random_string()).blocking()
        self.map.put(1, NestedPredicatePortableTest.Body("body1", NestedPredicatePortableTest.Limb("hand")))
        self.map.put(2, NestedPredicatePortableTest.Body("body2", NestedPredicatePortableTest.Limb("leg")))

    def tearDown(self):
        self.map.destroy()

    def test_adding_indexes(self):
        # single-attribute index
        self.map.add_index("name", True)

        # nested-attribute index
        self.map.add_index("limb.name", True)

    def test_single_attribute_query_portable_predicates(self):
        predicate = is_equal_to("limb.name", "hand")
        values = self.map.values(predicate)

        self.assertEqual(1, len(values))
        self.assertEqual("body1", values[0].name)

    def test_nested_attribute_query_sql_predicate(self):
        predicate = sql("limb.name == 'leg'")
        values = self.map.values(predicate)

        self.assertEqual(1, len(values))
        self.assertEqual("body2", values[0].name)
