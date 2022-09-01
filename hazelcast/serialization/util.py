import datetime
import decimal

from hazelcast.serialization.api import ObjectDataInput, ObjectDataOutput
from hazelcast.util import int_from_bytes, int_to_bytes


class IOUtil:
    @staticmethod
    def read_big_integer(inp: ObjectDataInput) -> int:
        length = inp.read_int()
        result = bytearray(length)
        inp.read_into(result, 0, length)
        return int_from_bytes(result)

    @staticmethod
    def write_big_integer(out: ObjectDataOutput, value: int) -> None:
        out.write_byte_array(int_to_bytes(value))

    @staticmethod
    def read_big_decimal(inp: ObjectDataInput) -> decimal.Decimal:
        unscaled_value = IOUtil.read_big_integer(inp)
        scale = inp.read_int()
        sign = 0 if unscaled_value >= 0 else 1
        return decimal.Decimal(
            (sign, tuple(int(digit) for digit in str(abs(unscaled_value))), -1 * scale)
        )

    @staticmethod
    def write_big_decimal(out: ObjectDataOutput, value: decimal.Decimal) -> None:
        sign, digits, exponent = value.as_tuple()
        unscaled_value = int("".join([str(digit) for digit in digits]))
        if sign == 1:
            unscaled_value = -1 * unscaled_value
        IOUtil.write_big_integer(out, unscaled_value)
        out.write_int(-1 * exponent)

    @staticmethod
    def read_time(inp: ObjectDataInput) -> datetime.time:
        return datetime.time(
            inp.read_byte(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_int() // 1000,  # server sends nanoseconds
        )

    @staticmethod
    def write_time(out: ObjectDataOutput, value: datetime.time) -> None:
        out.write_byte(value.hour)
        out.write_byte(value.minute)
        out.write_byte(value.second)
        out.write_int(value.microsecond * 1000)  # server expects nanoseconds

    @staticmethod
    def read_date(inp: ObjectDataInput) -> datetime.date:
        return datetime.date(
            inp.read_int(),
            inp.read_byte(),
            inp.read_byte(),
        )

    @staticmethod
    def write_date(out: ObjectDataOutput, value: datetime.date) -> None:
        out.write_int(value.year)
        out.write_byte(value.month)
        out.write_byte(value.day)

    @staticmethod
    def read_timestamp(inp: ObjectDataInput) -> datetime.datetime:
        return datetime.datetime(
            inp.read_int(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_int() // 1000,  # server sends nanoseconds
        )

    @staticmethod
    def write_timestamp(out: ObjectDataOutput, value: datetime.datetime) -> None:
        out.write_int(value.year)
        out.write_byte(value.month)
        out.write_byte(value.day)
        out.write_byte(value.hour)
        out.write_byte(value.minute)
        out.write_byte(value.second)
        out.write_int(value.microsecond * 1000)  # server expects nanoseconds

    @staticmethod
    def read_timestamp_with_timezone(inp: ObjectDataInput) -> datetime.datetime:
        return datetime.datetime(
            inp.read_int(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_int() // 1000,  # server sends nanoseconds
            datetime.timezone(datetime.timedelta(seconds=inp.read_int())),
        )

    @staticmethod
    def write_timestamp_with_timezone(out: ObjectDataOutput, value: datetime.datetime) -> None:
        out.write_int(value.year)
        out.write_byte(value.month)
        out.write_byte(value.day)
        out.write_byte(value.hour)
        out.write_byte(value.minute)
        out.write_byte(value.second)
        out.write_int(value.microsecond * 1000)  # server expects nanoseconds

        timezone_info = value.tzinfo
        if not timezone_info:
            out.write_int(0)
            return

        utc_offset = timezone_info.utcoffset(None)
        if utc_offset:
            out.write_int(int(utc_offset.total_seconds()))
        else:
            out.write_int(0)
