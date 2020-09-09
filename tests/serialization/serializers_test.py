# coding=utf-8
import datetime
import unittest
import uuid

from hazelcast import six
from hazelcast.core import HazelcastJsonValue
from hazelcast.config import SerializationConfig, INTEGER_TYPE
from hazelcast.errors import HazelcastSerializationError
from hazelcast.serialization import BE_INT, MAX_BYTE, MAX_SHORT, MAX_INT, MAX_LONG
from hazelcast.serialization.predicate import *
from hazelcast.serialization.service import SerializationServiceV1
from tests.base import SingleMemberTestCase
from tests.hzrc.ttypes import Lang
from tests.util import random_string

if not six.PY2:
    long = int


class A(object):
    def __init__(self, x):
        self.x = x

    def __eq__(self, other):
        return isinstance(other, A) and self.x == other.x

    def __ne__(self, other):
        return not self.__eq__(other)


class SerializersTest(unittest.TestCase):
    def setUp(self):
        self.service = SerializationServiceV1(SerializationConfig())

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
        self.validate(six.u("1⚐中💦2😭‍🙆😔5"))
        self.validate(six.u("Iñtërnâtiônàlizætiøn"))
        self.validate(six.u("\u0040\u0041\u01DF\u06A0\u12E0\u1D30"))

    def test_bytearray(self):
        self.validate(bytearray("abc".encode()))

    def test_none(self):
        self.validate(None)

    def test_hazelcast_json_value(self):
        self.validate(HazelcastJsonValue("{\"abc\": \"abc\", \"five\": 5}"))

    def test_uuid(self):
        self.validate(uuid.uuid4())

    def test_datetime(self):
        d = datetime.datetime.now()
        serialized = self.service.to_data(d)
        deserialized = self.service.to_object(serialized)
        self.assertEqual(d.timetuple(), deserialized.timetuple())

    def test_list(self):
        self.validate([1, 2.0, "a", None, bytearray("abc".encode()), [], [1, 2, 3]])

    def test_python_object(self):
        self.validate(A(123))

    def test_predicates(self):
        self.validate_predicate(sql("test"))
        self.validate_predicate(and_(true(), true()))
        self.validate_predicate(is_between("this", 0, 1))
        self.validate_predicate(is_equal_to("this", 10))
        self.validate_predicate(is_greater_than("this", 10))
        self.validate_predicate(is_greater_than_or_equal_to("this", 10))
        self.validate_predicate(is_less_than("this", 10))
        self.validate_predicate(is_less_than_or_equal_to("this", 10))
        self.validate_predicate(is_like("this", "*"))
        self.validate_predicate(is_ilike("this", "*"))
        self.validate_predicate(is_in("this", 10, 11, 12))
        self.validate_predicate(is_instance_of("java.lang.Serializable"))
        self.validate_predicate(is_not_equal_to("this", 10))
        self.validate_predicate(not_(true()))
        self.validate_predicate(or_(true(), true()))
        self.validate_predicate(matches_regex("this", "/abc/"))
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


class SerializersLiveTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config.cluster_name = cls.cluster.id
        return config

    def setUp(self):
        self.map = self.client.get_map(random_string()).blocking()

    def tearDown(self):
        self.map.clear()

    def get_from_server(self):
        script = """var StringArray = Java.type("java.lang.String[]");
        function foo() {
            var map = instance_0.getMap(\"""" + self.map.name + """\");
            var res = map.get("key");
            if (res.getClass().isArray()) {
                return Java.from(res);
            } else {
                return res;
            }
        }
        result = ""+foo();"""
        response = self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT)
        return response.result.decode("utf-8")

    def set_on_server(self, obj):
        script = """var map = instance_0.getMap(\"""" + self.map.name + """\");
        map.set("key", """ + obj + """);"""
        response = self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT)
        return response.success

    def replace_serialization_service(self, integer_type):
        config = SerializationConfig()
        config.default_integer_type = integer_type
        new_service = SerializationServiceV1(config)
        self.map._wrapped._to_data = new_service.to_data
        self.map._wrapped._to_object = new_service.to_object

    def test_bool(self):
        value = True
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = self.get_from_server() == "true"
        self.assertEqual(value, response)

    def test_byte(self):
        self.replace_serialization_service(INTEGER_TYPE.BYTE)
        value = (1 << 7) - 1
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = int(self.get_from_server())
        self.assertEqual(value, response)

    def test_short(self):
        self.replace_serialization_service(INTEGER_TYPE.SHORT)
        value = -1 * (1 << 15)
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = int(self.get_from_server())
        self.assertEqual(value, response)

    def test_int(self):
        value = (1 << 31) - 1
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = int(self.get_from_server())
        self.assertEqual(value, response)

    def test_long(self):
        self.replace_serialization_service(INTEGER_TYPE.LONG)
        value = -1 * (1 << 63)
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = long(self.get_from_server())
        self.assertEqual(value, response)

    def test_double(self):
        value = 123.0
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = float(self.get_from_server())
        self.assertEqual(value, response)

    def test_string(self):
        value = "value"
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = self.get_from_server()
        self.assertEqual(value, response)

    def test_utf_string(self):
        value = six.u("Iñtërnâtiônàlizætiøn")
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = self.get_from_server()
        self.assertEqual(value, response)

    def test_emoji(self):
        value = six.u("1⚐中💦2😭‍🙆😔5")
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = self.get_from_server()
        self.assertEqual(value, response)

    def test_utf_chars(self):
        value = six.u("\u0040\u0041\u01DF\u06A0\u12E0\u1D306")
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = self.get_from_server()
        self.assertEqual(value, response)

    def test_uuid(self):
        value = uuid.uuid4()
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = uuid.UUID(hex=self.get_from_server())
        self.assertEqual(value, response)

    def test_hjv(self):
        value = HazelcastJsonValue({"a": 3})
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = HazelcastJsonValue(self.get_from_server())
        self.assertEqual(value, response)

    def test_bytearray(self):
        value = bytearray("value".encode())
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = bytearray(map(int, self.get_from_server().split(",")))
        self.assertEqual(value, response)

    def test_datetime(self):
        value = datetime.datetime.now()
        self.map.set("key", value)
        self.assertEqual(value.timetuple(), self.map.get("key").timetuple())
        response = self.get_from_server()
        self.assertTrue(response.startswith(value.strftime("%a %b %d %H:%M:%S")))

    def test_big_integer(self):
        self.replace_serialization_service(INTEGER_TYPE.BIG_INT)
        value = 1 << 128
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = long(self.get_from_server())
        self.assertEqual(value, response)

    def test_variable_integer(self):
        self.replace_serialization_service(INTEGER_TYPE.VAR)
        value = MAX_BYTE
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = long(self.get_from_server())
        self.assertEqual(value, response)

        value = MAX_SHORT
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = long(self.get_from_server())
        self.assertEqual(value, response)

        value = MAX_INT
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = long(self.get_from_server())
        self.assertEqual(value, response)

        value = MAX_LONG
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = long(self.get_from_server())
        self.assertEqual(value, response)

        value = 1234567890123456789012345678901234567890
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = long(self.get_from_server())
        self.assertEqual(value, response)

    def test_list(self):
        value = [1, 2, 3]
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = self.get_from_server()
        self.assertEqual(value, list(map(int, response[1:-1].split(", "))))

    def test_bool_from_server(self):
        self.assertTrue(self.set_on_server("true"))
        self.assertEqual(True, self.map.get("key"))

    def test_byte_from_server(self):
        self.assertTrue(self.set_on_server("new java.lang.Byte(-23)"))
        self.assertEqual(-23, self.map.get("key"))

    def test_char_from_server(self):
        self.assertTrue(self.set_on_server("new java.lang.Character('x')"))
        self.assertEqual("x", self.map.get("key"))

    def test_short_from_server(self):
        self.assertTrue(self.set_on_server("new java.lang.Short(23)"))
        self.assertEqual(23, self.map.get("key"))

    def test_integer_from_server(self):
        self.assertTrue(self.set_on_server("new java.lang.Integer(" + str(1 << 30) + ")"))
        self.assertEqual(1 << 30, self.map.get("key"))

    def test_long_from_server(self):
        self.assertTrue(self.set_on_server("new java.lang.Long(-1 * " + str(1 << 63) + ")"))
        self.assertEqual(-1 * (1 << 63), self.map.get("key"))

    def test_float_from_server(self):
        self.assertTrue(self.set_on_server("new java.lang.Float(32.0)"))
        self.assertEqual(32.0, self.map.get("key"))

    def test_double_from_server(self):
        self.assertTrue(self.set_on_server("new java.lang.Double(-12332.0)"))
        self.assertEqual(-12332.0, self.map.get("key"))

    def test_string_from_server(self):
        self.assertTrue(self.set_on_server(six.u("\"1⚐中💦2😭‍🙆😔5\"")))
        self.assertEqual(six.u("1⚐中💦2😭‍🙆😔5"), self.map.get("key"))

    def test_uuid_from_server(self):
        self.assertTrue(self.set_on_server("new java.util.UUID(0, 1)"))
        self.assertEqual(uuid.UUID(int=1), self.map.get("key"))

    def test_hjv_from_server(self):
        self.assertTrue(self.set_on_server("new com.hazelcast.core.HazelcastJsonValue(\"{\\\"a\\\": 3}\")"))
        self.assertEqual(HazelcastJsonValue({"a": 3}), self.map.get("key"))

    def test_bool_array_from_server(self):
        self.assertTrue(self.set_on_server("Java.to([true, false], \"boolean[]\")"))
        self.assertEqual([True, False], self.map.get("key"))

    def test_byte_array_from_server(self):
        self.assertTrue(self.set_on_server("Java.to([3, 123], \"byte[]\")"))
        self.assertEqual(bytearray([3, 123]), self.map.get("key"))

    def test_char_array_from_server(self):
        self.assertTrue(self.set_on_server("Java.to(['x', 'y'], \"char[]\")"))
        self.assertEqual(["x", "y"], self.map.get("key"))

    def test_short_array_from_server(self):
        self.assertTrue(self.set_on_server("Java.to([1323, -1232], \"short[]\")"))
        self.assertEqual([1323, -1232], self.map.get("key"))

    def test_int_array_from_server(self):
        self.assertTrue(self.set_on_server("Java.to([2147483647, -2147483648], \"int[]\")"))
        self.assertEqual([2147483647, -2147483648], self.map.get("key"))

    def test_long_array_from_server(self):
        self.assertTrue(self.set_on_server("Java.to([1152921504606846976, -1152921504606846976], \"long[]\")"))
        self.assertEqual([1152921504606846976, -1152921504606846976], self.map.get("key"))

    def test_float_array_from_server(self):
        self.assertTrue(self.set_on_server("Java.to([3123.0, -123.0], \"float[]\")"))
        self.assertEqual([3123.0, -123.0], self.map.get("key"))

    def test_double_array_from_server(self):
        self.assertTrue(self.set_on_server("Java.to([3123.0, -123.0], \"double[]\")"))
        self.assertEqual([3123.0, -123.0], self.map.get("key"))

    def test_string_array_from_server(self):
        self.assertTrue(self.set_on_server(six.u("Java.to([\"hey\", \"1⚐中💦2😭‍🙆😔5\"], \"java.lang.String[]\")")))
        self.assertEqual(["hey", six.u("1⚐中💦2😭‍🙆😔5")], self.map.get("key"))

    def test_date_from_server(self):
        self.assertTrue(self.set_on_server("new java.util.Date(100, 11, 15, 23, 59, 49)"))
        # server adds 1900 to year. Also, month is 0-based for server and 1-based for the client
        self.assertEqual(datetime.datetime(2000, 12, 15, 23, 59, 49), self.map.get("key"))

    def test_big_integer_from_server(self):
        self.assertTrue(self.set_on_server("new java.math.BigInteger(\"12\", 10)"))
        self.assertEqual(12, self.map.get("key"))

        self.assertTrue(self.set_on_server("new java.math.BigInteger(\"-13\", 10)"))
        self.assertEqual(-13, self.map.get("key"))

        self.assertTrue(
            self.set_on_server("new java.math.BigInteger(\"1234567890123456789012345678901234567890\", 10)"))
        self.assertEqual(1234567890123456789012345678901234567890, self.map.get("key"))

        self.assertTrue(
            self.set_on_server("new java.math.BigInteger(\"-1234567890123456789012345678901234567890\", 10)"))
        self.assertEqual(-1234567890123456789012345678901234567890, self.map.get("key"))

    def test_java_class_from_server(self):
        self.assertTrue(self.set_on_server("java.lang.String.class"))
        self.assertEqual("java.lang.String", self.map.get("key"))

    def test_array_list_from_server(self):
        script = """var list = new java.util.ArrayList();
        list.add(1);
        list.add(2);
        list.add(3);
        var map = instance_0.getMap(\"""" + self.map.name + """\");
        map.set("key", list);"""
        response = self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT)
        self.assertTrue(response.success)
        self.assertEqual([1, 2, 3], self.map.get("key"))

    def test_linked_list_from_server(self):
        script = """var list = new java.util.LinkedList();
        list.add("a");
        list.add("b");
        list.add("c");
        var map = instance_0.getMap(\"""" + self.map.name + """\");
        map.set("key", list);"""
        response = self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT)
        self.assertTrue(response.success)
        self.assertEqual(["a", "b", "c"], self.map.get("key"))
