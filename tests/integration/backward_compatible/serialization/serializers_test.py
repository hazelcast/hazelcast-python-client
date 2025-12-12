# coding=utf-8
import datetime
import decimal
import uuid

from hazelcast import HazelcastClient
from hazelcast.config import IntType
from hazelcast.core import HazelcastJsonValue
from hazelcast.serialization import MAX_BYTE, MAX_SHORT, MAX_INT, MAX_LONG
from tests.base import SingleMemberTestCase
from tests.hzrc.ttypes import Lang
from tests.util import (
    random_string,
    skip_if_client_version_newer_than_or_equal,
    skip_if_client_version_older_than,
    skip_if_server_version_older_than,
)


class SerializersLiveTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    def setUp(self):
        self.disposables = []
        self.map = self.client.get_map(random_string()).blocking()

    def tearDown(self):
        self.map.clear()
        for disposable in self.disposables:
            disposable()

    def get_from_server(self):
        script = (
            """
        function foo() {
            var map = instance_0.getMap("%s");
            var res = map.get("key");
            if (res.getClass().isArray()) {
                return Java.from(res);
            } else {
                return res;
            }
        }
        result = ""+foo();"""
            % self.map.name
        )
        response = self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT)
        return response.result.decode("utf-8")

    def set_on_server(self, obj):
        script = """
        var map = instance_0.getMap("%s");
        map.set("key", %s);""" % (
            self.map.name,
            obj,
        )
        response = self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT)
        return response.success

    def create_new_map_with(self, default_int_type):
        client = HazelcastClient(cluster_name=self.cluster.id, default_int_type=default_int_type)
        self.disposables.append(lambda: client.shutdown())
        self.map = client.get_map(random_string()).blocking()

    def test_bool(self):
        value = True
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = self.get_from_server() == "true"
        self.assertEqual(value, response)

    def test_byte(self):
        self.create_new_map_with(IntType.BYTE)
        value = (1 << 7) - 1
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = int(self.get_from_server())
        self.assertEqual(value, response)

    def test_short(self):
        self.create_new_map_with(IntType.SHORT)
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
        self.create_new_map_with(IntType.LONG)
        value = -1 * (1 << 63)
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = int(self.get_from_server())
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
        value = "Iñtërnâtiônàlizætiøn"
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = self.get_from_server()
        self.assertEqual(value, response)

    def test_emoji(self):
        value = "1⚐中💦2😭‍🙆😔5"
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = self.get_from_server()
        self.assertEqual(value, response)

    def test_utf_chars(self):
        value = "\u0040\u0041\u01DF\u06A0\u12E0\u1D306"
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
        skip_if_client_version_newer_than_or_equal(self, "5.0")
        value = datetime.datetime.now()
        self.map.set("key", value)
        self.assertEqual(value.timetuple(), self.map.get("key").timetuple())
        response = self.get_from_server()
        self.assertTrue(response.startswith(value.strftime("%a %b %d %H:%M:%S")))

    def test_big_integer(self):
        self.create_new_map_with(IntType.BIG_INT)
        value = 1 << 128
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = int(self.get_from_server())
        self.assertEqual(value, response)

    def test_variable_integer(self):
        self.create_new_map_with(IntType.VAR)
        value = MAX_BYTE
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = int(self.get_from_server())
        self.assertEqual(value, response)

        value = MAX_SHORT
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = int(self.get_from_server())
        self.assertEqual(value, response)

        value = MAX_INT
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = int(self.get_from_server())
        self.assertEqual(value, response)

        value = MAX_LONG
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = int(self.get_from_server())
        self.assertEqual(value, response)

        value = 1234567890123456789012345678901234567890
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = int(self.get_from_server())
        self.assertEqual(value, response)

    def test_decimal(self):
        skip_if_client_version_older_than(self, "5.0")
        decimal_value = "1234567890123456789012345678901234567890.987654321"
        value = decimal.Decimal(decimal_value)
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = self.get_from_server()
        self.assertEqual(decimal_value, response)

    def test_list(self):
        value = [1, 2, 3]
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = self.get_from_server()
        self.assertEqual(value, list(map(int, response[1:-1].split(", "))))

    def test_datetime_date(self):
        skip_if_client_version_older_than(self, "5.0")
        skip_if_server_version_older_than(self, self.client, "5.0")
        value = datetime.datetime.now().date()
        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = self.get_from_server()
        self.assertEqual(response, value.strftime("%Y-%m-%d"))

    def test_datetime_time(self):
        skip_if_client_version_older_than(self, "5.0")
        skip_if_server_version_older_than(self, self.client, "5.0")
        value = datetime.datetime.now()
        if value.microsecond % 1000 == 0:
            # A little hack for Windows. Time is precise to the
            # milliseconds there. If we send the microseconds
            # we have now, due to trailing zeros, get_from_server()
            # call below will return the string representation with
            # 3 digits for the microseconds. But, Python always expects
            # 6 digits. So, the assertion will fail. To fix that,
            # we add 1 microseconds to the value, so that in the Java
            # side, nanoseconds representation will only have 3 trailing
            # zeros, and will send the data as we want.
            value = value + datetime.timedelta(microseconds=1)
        value = value.time()

        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = self.get_from_server()
        self.assertEqual(response, value.strftime("%H:%M:%S.%f"))

    def test_datetime_datetime(self):
        skip_if_client_version_older_than(self, "5.0")
        skip_if_server_version_older_than(self, self.client, "5.0")
        value = datetime.datetime.now(datetime.timezone(datetime.timedelta(seconds=1800)))
        if value.microsecond % 1000 == 0:
            # A little hack for Windows. Time is precise to the
            # milliseconds there. If we send the microseconds
            # we have now, due to trailing zeros, get_from_server()
            # call below will return the string representation with
            # 3 digits for the microseconds. But, Python always expects
            # 6 digits. So, the assertion will fail. To fix that,
            # we add 1 microseconds to the value, so that in the Java
            # side, nanoseconds representation will only have 3 trailing
            # zeros, and will send the data as we want.
            value = value + datetime.timedelta(microseconds=1)

        self.map.set("key", value)
        self.assertEqual(value, self.map.get("key"))
        response = self.get_from_server()
        expected = value.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        # Java sends offset string with a : in between hour and minute
        expected = "%s:%s" % (expected[:-2], expected[-2:])
        self.assertEqual(response, expected)

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
        self.assertTrue(self.set_on_server('"1⚐中💦2😭‍🙆😔5"'))
        self.assertEqual("1⚐中💦2😭‍🙆😔5", self.map.get("key"))

    def test_uuid_from_server(self):
        self.assertTrue(self.set_on_server("new java.util.UUID(0, 1)"))
        self.assertEqual(uuid.UUID(int=1), self.map.get("key"))

    def test_hjv_from_server(self):
        self.assertTrue(
            self.set_on_server('new com.hazelcast.core.HazelcastJsonValue("{\\"a\\": 3}")')
        )
        self.assertEqual(HazelcastJsonValue({"a": 3}), self.map.get("key"))

    def test_bool_array_from_server(self):
        self.assertTrue(self.set_on_server('Java.to([true, false], "boolean[]")'))
        self.assertEqual([True, False], self.map.get("key"))

    def test_byte_array_from_server(self):
        self.assertTrue(self.set_on_server('Java.to([3, 123], "byte[]")'))
        self.assertEqual(bytearray([3, 123]), self.map.get("key"))

    def test_char_array_from_server(self):
        self.assertTrue(self.set_on_server("Java.to(['x', 'y'], \"char[]\")"))
        self.assertEqual(["x", "y"], self.map.get("key"))

    def test_short_array_from_server(self):
        self.assertTrue(self.set_on_server('Java.to([1323, -1232], "short[]")'))
        self.assertEqual([1323, -1232], self.map.get("key"))

    def test_int_array_from_server(self):
        self.assertTrue(self.set_on_server('Java.to([2147483647, -2147483648], "int[]")'))
        self.assertEqual([2147483647, -2147483648], self.map.get("key"))

    def test_long_array_from_server(self):
        self.assertTrue(
            self.set_on_server('Java.to([1152921504606846976, -1152921504606846976], "long[]")')
        )
        self.assertEqual([1152921504606846976, -1152921504606846976], self.map.get("key"))

    def test_float_array_from_server(self):
        self.assertTrue(self.set_on_server('Java.to([3123.0, -123.0], "float[]")'))
        self.assertEqual([3123.0, -123.0], self.map.get("key"))

    def test_double_array_from_server(self):
        self.assertTrue(self.set_on_server('Java.to([3123.0, -123.0], "double[]")'))
        self.assertEqual([3123.0, -123.0], self.map.get("key"))

    def test_string_array_from_server(self):
        self.assertTrue(self.set_on_server('Java.to(["hey", "1⚐中💦2😭‍🙆😔5"], "java.lang.String[]")'))
        self.assertEqual(["hey", "1⚐中💦2😭‍🙆😔5"], self.map.get("key"))

    def test_date_from_server(self):
        skip_if_client_version_newer_than_or_equal(self, "5.0")
        self.assertTrue(self.set_on_server("new java.util.Date(100, 11, 15, 23, 59, 49)"))
        # server adds 1900 to year. Also, month is 0-based for server and 1-based for the client
        self.assertEqual(datetime.datetime(2000, 12, 15, 23, 59, 49), self.map.get("key"))

    def test_big_integer_from_server(self):
        self.assertTrue(self.set_on_server('new java.math.BigInteger("12", 10)'))
        self.assertEqual(12, self.map.get("key"))

        self.assertTrue(self.set_on_server('new java.math.BigInteger("-13", 10)'))
        self.assertEqual(-13, self.map.get("key"))

        self.assertTrue(
            self.set_on_server(
                'new java.math.BigInteger("1234567890123456789012345678901234567890", 10)'
            )
        )
        self.assertEqual(1234567890123456789012345678901234567890, self.map.get("key"))

        self.assertTrue(
            self.set_on_server(
                'new java.math.BigInteger("-1234567890123456789012345678901234567890", 10)'
            )
        )
        self.assertEqual(-1234567890123456789012345678901234567890, self.map.get("key"))

    def test_big_decimal_from_server(self):
        skip_if_client_version_older_than(self, "5.0")
        self.assertTrue(self.set_on_server('new java.math.BigDecimal("12.12")'))
        self.assertEqual(decimal.Decimal("12.12"), self.map.get("key"))

        self.assertTrue(self.set_on_server('new java.math.BigDecimal("-13.13")'))
        self.assertEqual(decimal.Decimal("-13.13"), self.map.get("key"))

        self.assertTrue(
            self.set_on_server(
                'new java.math.BigDecimal("1234567890123456789012345678901234567890.123456789")'
            )
        )
        self.assertEqual(
            decimal.Decimal("1234567890123456789012345678901234567890.123456789"),
            self.map.get("key"),
        )

        self.assertTrue(
            self.set_on_server(
                'new java.math.BigDecimal("-1234567890123456789012345678901234567890.123456789")'
            )
        )
        self.assertEqual(
            decimal.Decimal("-1234567890123456789012345678901234567890.123456789"),
            self.map.get("key"),
        )

    def test_java_class_from_server(self):
        self.assertTrue(self.set_on_server("java.lang.String.class"))
        self.assertEqual("java.lang.String", self.map.get("key"))

    def test_array_list_from_server(self):
        script = (
            """
        var list = new java.util.ArrayList();
        list.add(1);
        list.add(2);
        list.add(3);
        var map = instance_0.getMap("%s");
        map.set("key", list);"""
            % self.map.name
        )
        response = self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT)
        self.assertTrue(response.success)
        self.assertEqual([1, 2, 3], self.map.get("key"))

    def test_linked_list_from_server(self):
        script = (
            """
        var list = new java.util.LinkedList();
        list.add("a");
        list.add("b");
        list.add("c");
        var map = instance_0.getMap("%s");
        map.set("key", list);"""
            % self.map.name
        )
        response = self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT)
        self.assertTrue(response.success)
        self.assertEqual(["a", "b", "c"], self.map.get("key"))

    def test_local_date_from_server(self):
        skip_if_client_version_older_than(self, "5.0")
        skip_if_server_version_older_than(self, self.client, "5.0")
        self.assertTrue(self.set_on_server("java.time.LocalDate.of(2000, 12, 15)"))
        self.assertEqual(datetime.date(2000, 12, 15), self.map.get("key"))

    def test_local_time_from_server(self):
        skip_if_client_version_older_than(self, "5.0")
        skip_if_server_version_older_than(self, self.client, "5.0")
        self.assertTrue(self.set_on_server("java.time.LocalTime.of(18, 3, 35)"))
        self.assertEqual(datetime.time(18, 3, 35), self.map.get("key"))

    def test_local_date_time_from_server(self):
        skip_if_client_version_older_than(self, "5.0")
        skip_if_server_version_older_than(self, self.client, "5.0")
        self.assertTrue(
            self.set_on_server("java.time.LocalDateTime.of(2021, 8, 24, 0, 59, 55, 987654000)")
        )
        self.assertEqual(datetime.datetime(2021, 8, 24, 0, 59, 55, 987654), self.map.get("key"))

    def test_offset_date_time_from_server(self):
        skip_if_client_version_older_than(self, "5.0")
        skip_if_server_version_older_than(self, self.client, "5.0")
        self.assertTrue(
            self.set_on_server(
                "java.time.OffsetDateTime.of(2021, 8, 24, 0, 59, 55, 987654000, "
                "java.time.ZoneOffset.ofTotalSeconds(2400))"
            )
        )
        self.assertEqual(
            datetime.datetime(
                2021, 8, 24, 0, 59, 55, 987654, datetime.timezone(datetime.timedelta(seconds=2400))
            ),
            self.map.get("key"),
        )
