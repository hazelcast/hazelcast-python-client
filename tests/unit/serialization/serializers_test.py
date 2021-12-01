# coding=utf-8
import datetime
import decimal
import unittest
import uuid

from hazelcast.core import HazelcastJsonValue
from hazelcast.config import _Config
from hazelcast.errors import HazelcastSerializationError
from hazelcast.serialization import BE_INT
from hazelcast.predicate import *
from hazelcast.serialization.service import SerializationServiceV1


class A(object):
    def __init__(self, x):
        self.x = x

    def __eq__(self, other):
        return isinstance(other, A) and self.x == other.x

    def __ne__(self, other):
        return not self.__eq__(other)


class SerializersTest(unittest.TestCase):
    def setUp(self):
        self.service = SerializationServiceV1(_Config())

    def tearDown(self):
        self.service.destroy()

    def test_integer(self):
        self.validate(14)
        self.validate(-14)

    def test_float(self):
        self.validate(545.3)
        self.validate(-545.3)

    def test_large_int_raises(self):
        with self.assertRaises(HazelcastSerializationError):
            self.validate(1 << 63)

    def test_bool(self):
        self.validate(True)
        self.validate(False)

    def test_string(self):
        self.validate("")
        self.validate("client")
        self.validate("1⚐中💦2😭‍🙆😔5")
        self.validate("Iñtërnâtiônàlizætiøn")
        self.validate("\u0040\u0041\u01DF\u06A0\u12E0\u1D30")

    def test_bytearray(self):
        self.validate(bytearray("abc".encode()))

    def test_none(self):
        self.validate(None)

    def test_hazelcast_json_value(self):
        self.validate(HazelcastJsonValue('{"abc": "abc", "five": 5}'))

    def test_uuid(self):
        self.validate(uuid.uuid4())

    def test_datetime_datetime(self):
        d = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=-15)))
        serialized = self.service.to_data(d)
        deserialized = self.service.to_object(serialized)
        self.assertEqual(d, deserialized)

    def test_datetime_date(self):
        d = datetime.datetime.now().date()
        serialized = self.service.to_data(d)
        deserialized = self.service.to_object(serialized)
        self.assertEqual(d, deserialized)

    def test_datetime_time(self):
        d = datetime.datetime.now().time()
        serialized = self.service.to_data(d)
        deserialized = self.service.to_object(serialized)
        self.assertEqual(d, deserialized)

    def test_decimal(self):
        d = decimal.Decimal("-123456.789")
        serialized = self.service.to_data(d)
        deserialized = self.service.to_object(serialized)
        self.assertEqual(d, deserialized)

    def test_list(self):
        self.validate([1, 2.0, "a", None, bytearray("abc".encode()), [], [1, 2, 3]])

    def test_python_object(self):
        self.validate(A(123))

    def test_predicates(self):
        self.validate_predicate(sql("test"))
        self.validate_predicate(and_(true(), true()))
        self.validate_predicate(between("this", 0, 1))
        self.validate_predicate(equal("this", 10))
        self.validate_predicate(greater("this", 10))
        self.validate_predicate(greater_or_equal("this", 10))
        self.validate_predicate(less("this", 10))
        self.validate_predicate(less_or_equal("this", 10))
        self.validate_predicate(like("this", "*"))
        self.validate_predicate(ilike("this", "*"))
        self.validate_predicate(in_("this", 10, 11, 12))
        self.validate_predicate(instance_of("java.lang.Serializable"))
        self.validate_predicate(not_equal("this", 10))
        self.validate_predicate(not_(true()))
        self.validate_predicate(or_(true(), true()))
        self.validate_predicate(regex("this", "/abc/"))
        self.validate_predicate(true())
        self.validate_predicate(false())

    def validate(self, value):
        serialized = self.service.to_data(value)
        deserialized = self.service.to_object(serialized)
        self.assertEqual(value, deserialized)

    def validate_predicate(self, predicate):
        serialized = self.service.to_data(predicate)
        self.assertEqual(-2, serialized.get_type())  # Identified
        b = serialized.to_bytes()
        # 4(partition hash) + 4(serializer type) + 1(is_identified) + 4(factory id) + 4(class id) + payload(if any)
        self.assertTrue(len(b) >= 17)
        self.assertEqual(predicate.get_factory_id(), BE_INT.unpack_from(b, 9)[0])
        self.assertEqual(predicate.get_class_id(), BE_INT.unpack_from(b, 13)[0])
