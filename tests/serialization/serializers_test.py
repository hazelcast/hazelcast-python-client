import binascii

from hzrc.ttypes import Lang

from hazelcast.config import SerializationConfig, INTEGER_TYPE
from hazelcast.serialization.data import Data
from hazelcast.serialization.serialization_const import CONSTANT_TYPE_DOUBLE
from hazelcast.serialization.service import SerializationServiceV1
from tests.base import SingleMemberTestCase


class SerializersTestCase(SingleMemberTestCase):
    def setUp(self):
        config = SerializationConfig()
        config.default_integer_type = INTEGER_TYPE.BIG_INT
        self.service = SerializationServiceV1(serialization_config=config)

    def tearDown(self):
        self.service.destroy()

    def test_none_serializer(self):
        none = None
        data_n = self.service.to_data(none)
        self.assertIsNone(data_n)
        self.assertIsNone(self.service.to_object(Data()))

    def test_boolean_serializer(self):
        true = True
        false = False
        data_t = self.service.to_data(true)
        data_f = self.service.to_data(false)

        obj_t = self.service.to_object(data_t)
        obj_f = self.service.to_object(data_f)
        self.assertEqual(true, obj_t)
        self.assertEqual(false, obj_f)

    def test_char_type_serializer(self):
        buff = bytearray(binascii.unhexlify("00000000fffffffb00e7"))
        data = Data(buff)
        obj = self.service.to_object(data)
        self.assertEqual(unichr(0x00e7), obj)

    def test_float(self):
        buff = bytearray(binascii.unhexlify("00000000fffffff700000000"))
        data = Data(buff)
        obj = self.service.to_object(data)
        self.assertEqual(0.0, obj)

    def test_double(self):
        double = 1.0
        data = self.service.to_data(double)
        obj = self.service.to_object(data)
        self.assertEqual(data.get_type(), CONSTANT_TYPE_DOUBLE)
        self.assertEqual(double, obj)

    def test_datetime(self):
        year = 2000
        month = 11
        day = 15
        hour = 23
        minute = 59
        second = 49
        script = """
from java.util import Date, Calendar
cal = Calendar.getInstance()
cal.set({}, ({}-1), {}, {}, {}, {})
result=instance_0.getSerializationService().toBytes(cal.getTime())
""".format(year, month, day, hour, minute, second)
        response = self.rc.executeOnController(self.cluster.id, script, Lang.PYTHON)
        data = Data(response.result)
        val = self.service.to_object(data)
        self.assertEqual(year, val.year)
        self.assertEqual(month, val.month)
        self.assertEqual(day, val.day)
        self.assertEqual(hour, val.hour)
        self.assertEqual(minute, val.minute)
        self.assertEqual(second, val.second)

    def test_big_int_small(self):
        self._big_int_test(12)

    def test_big_int_small_neg(self):
        self._big_int_test(-13)

    def test_big_int(self):
        self._big_int_test(1234567890123456789012345678901234567890)

    def test_big_int_neg(self):
        self._big_int_test(-1234567890123456789012345678901234567890)

    def _big_int_test(self, big_int):
        script = """from java.math import BigInteger
result=instance_0.getSerializationService().toBytes(BigInteger("{}",10))""".format(big_int)
        response = self.rc.executeOnController(self.cluster.id, script, Lang.PYTHON)
        data = Data(response.result)
        val = self.service.to_object(data)
        data_local = self.service.to_data(big_int)

        self.assertEqual(binascii.hexlify(data._buffer), binascii.hexlify(data_local._buffer))
        self.assertEqual(big_int, val)
