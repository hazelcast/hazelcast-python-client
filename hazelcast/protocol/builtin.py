import uuid
from datetime import date, time, datetime, timedelta, timezone
from decimal import Decimal

from hazelcast.protocol.client_message import (
    NULL_FRAME_BUF,
    BEGIN_FRAME_BUF,
    END_FRAME_BUF,
    SIZE_OF_FRAME_LENGTH_AND_FLAGS,
    _IS_FINAL_FLAG,
    NULL_FINAL_FRAME_BUF,
    END_FINAL_FRAME_BUF,
)
from hazelcast.serialization.bits import (
    LONG_SIZE_IN_BYTES,
    UUID_SIZE_IN_BYTES,
    LE_INT,
    LE_LONG,
    BOOLEAN_SIZE_IN_BYTES,
    INT_SIZE_IN_BYTES,
    LE_ULONG,
    LE_UINT16,
    LE_INT8,
    UUID_MSB_SHIFT,
    UUID_LSB_MASK,
    BYTE_SIZE_IN_BYTES,
    SHORT_SIZE_IN_BYTES,
    LE_INT16,
    FLOAT_SIZE_IN_BYTES,
    LE_FLOAT,
    LE_DOUBLE,
    DOUBLE_SIZE_IN_BYTES,
)
from hazelcast.serialization.data import Data
from hazelcast.util import int_from_bytes

_LOCAL_DATE_SIZE_IN_BYTES = INT_SIZE_IN_BYTES + BYTE_SIZE_IN_BYTES * 2
_LOCAL_TIME_SIZE_IN_BYTES = BYTE_SIZE_IN_BYTES * 3 + INT_SIZE_IN_BYTES
_LOCAL_DATE_TIME_SIZE_IN_BYTES = _LOCAL_DATE_SIZE_IN_BYTES + _LOCAL_TIME_SIZE_IN_BYTES
_OFFSET_DATE_TIME_SIZE_IN_BYTES = _LOCAL_DATE_TIME_SIZE_IN_BYTES + INT_SIZE_IN_BYTES


class CodecUtil(object):
    @staticmethod
    def fast_forward_to_end_frame(msg):
        # We are starting from 1 because of the BEGIN_FRAME we read
        # in the beginning of the decode method
        num_expected_end_frames = 1
        while num_expected_end_frames != 0:
            frame = msg.next_frame()
            if frame.is_end_frame():
                num_expected_end_frames -= 1
            elif frame.is_begin_frame():
                num_expected_end_frames += 1

    @staticmethod
    def encode_nullable(buf, value, encoder, is_final=False):
        if value is None:
            if is_final:
                buf.extend(NULL_FINAL_FRAME_BUF)
            else:
                buf.extend(NULL_FRAME_BUF)
        else:
            encoder(buf, value, is_final)

    @staticmethod
    def decode_nullable(msg, decoder):
        if CodecUtil.next_frame_is_null_frame(msg):
            return None
        else:
            return decoder(msg)

    @staticmethod
    def next_frame_is_data_structure_end_frame(msg):
        return msg.peek_next_frame().is_end_frame()

    @staticmethod
    def next_frame_is_null_frame(msg):
        is_null = msg.peek_next_frame().is_null_frame()
        if is_null:
            msg.next_frame()
        return is_null


class ByteArrayCodec(object):
    @staticmethod
    def encode(buf, value, is_final=False):
        header = bytearray(SIZE_OF_FRAME_LENGTH_AND_FLAGS)
        LE_INT.pack_into(header, 0, SIZE_OF_FRAME_LENGTH_AND_FLAGS + len(value))
        if is_final:
            LE_UINT16.pack_into(header, INT_SIZE_IN_BYTES, _IS_FINAL_FLAG)
        buf.extend(header)
        buf.extend(value)

    @staticmethod
    def decode(msg):
        return msg.next_frame().buf


class DataCodec(object):
    @staticmethod
    def encode(buf, value, is_final=False):
        value_bytes = value.to_bytes()
        header = bytearray(SIZE_OF_FRAME_LENGTH_AND_FLAGS)
        LE_INT.pack_into(header, 0, SIZE_OF_FRAME_LENGTH_AND_FLAGS + len(value_bytes))
        if is_final:
            LE_UINT16.pack_into(header, INT_SIZE_IN_BYTES, _IS_FINAL_FLAG)
        buf.extend(header)
        buf.extend(value_bytes)

    @staticmethod
    def decode(msg):
        return Data(msg.next_frame().buf)

    @staticmethod
    def encode_nullable(buf, value, is_final=False):
        if value is None:
            if is_final:
                buf.extend(NULL_FINAL_FRAME_BUF)
            else:
                buf.extend(NULL_FRAME_BUF)
        else:
            DataCodec.encode(buf, value, is_final)

    @staticmethod
    def decode_nullable(msg):
        if CodecUtil.next_frame_is_null_frame(msg):
            return None
        else:
            return DataCodec.decode(msg)


class EntryListCodec(object):
    @staticmethod
    def encode(buf, entries, key_encoder, value_encoder, is_final=False):
        buf.extend(BEGIN_FRAME_BUF)
        for key, value in entries:
            key_encoder(buf, key)
            value_encoder(buf, value)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def encode_nullable(buf, entries, key_encoder, value_encoder, is_final=False):
        if entries is None:
            if is_final:
                buf.extend(NULL_FINAL_FRAME_BUF)
            else:
                buf.extend(NULL_FRAME_BUF)
        else:
            EntryListCodec.encode(buf, entries, key_encoder, value_encoder, is_final)

    @staticmethod
    def decode(msg, key_decoder, value_decoder):
        result = []
        msg.next_frame()
        while not CodecUtil.next_frame_is_data_structure_end_frame(msg):
            key = key_decoder(msg)
            value = value_decoder(msg)
            result.append((key, value))

        msg.next_frame()
        return result

    @staticmethod
    def decode_nullable(msg, key_decoder, value_decoder):
        if CodecUtil.next_frame_is_null_frame(msg):
            return None
        else:
            return EntryListCodec.decode(msg, key_decoder, value_decoder)


_UUID_LONG_ENTRY_SIZE_IN_BYTES = UUID_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES


class EntryListUUIDLongCodec(object):
    @staticmethod
    def encode(buf, entries, is_final=False):
        n = len(entries)
        size = SIZE_OF_FRAME_LENGTH_AND_FLAGS + n * _UUID_LONG_ENTRY_SIZE_IN_BYTES
        b = bytearray(size)
        LE_INT.pack_into(b, 0, size)
        if is_final:
            LE_UINT16.pack_into(b, INT_SIZE_IN_BYTES, _IS_FINAL_FLAG)
        for i in range(n):
            key, value = entries[i]
            o = SIZE_OF_FRAME_LENGTH_AND_FLAGS + i * _UUID_LONG_ENTRY_SIZE_IN_BYTES
            FixSizedTypesCodec.encode_uuid(b, o, key)
            FixSizedTypesCodec.encode_long(b, o + UUID_SIZE_IN_BYTES, value)
        buf.extend(b)

    @staticmethod
    def decode(msg):
        b = msg.next_frame().buf
        n = len(b) // _UUID_LONG_ENTRY_SIZE_IN_BYTES
        result = []
        for i in range(n):
            o = i * _UUID_LONG_ENTRY_SIZE_IN_BYTES
            key = FixSizedTypesCodec.decode_uuid(b, o)
            value = FixSizedTypesCodec.decode_long(b, o + UUID_SIZE_IN_BYTES)
            result.append((key, value))
        return result


class EntryListUUIDListIntegerCodec(object):
    @staticmethod
    def encode(buf, entries, is_final=False):
        keys = []
        buf.extend(BEGIN_FRAME_BUF)
        for key, value in entries:
            keys.append(key)
            ListIntegerCodec.encode(buf, value)
        buf.extend(END_FRAME_BUF)
        ListUUIDCodec.encode(buf, keys, is_final)

    @staticmethod
    def decode(msg):
        values = ListMultiFrameCodec.decode(msg, ListIntegerCodec.decode)
        keys = ListUUIDCodec.decode(msg)
        result = []
        n = len(keys)
        for i in range(n):
            result.append((keys[i], values[i]))
        return result


class FixSizedTypesCodec(object):
    @staticmethod
    def encode_int(buf, offset, value):
        LE_INT.pack_into(buf, offset, value)

    @staticmethod
    def decode_int(buf, offset):
        return LE_INT.unpack_from(buf, offset)[0]

    @staticmethod
    def encode_long(buf, offset, value):
        LE_LONG.pack_into(buf, offset, value)

    @staticmethod
    def decode_long(buf, offset):
        return LE_LONG.unpack_from(buf, offset)[0]

    @staticmethod
    def encode_boolean(buf, offset, value):
        if value:
            LE_INT8.pack_into(buf, offset, 1)
        else:
            LE_INT8.pack_into(buf, offset, 0)

    @staticmethod
    def decode_boolean(buf, offset):
        return LE_INT8.unpack_from(buf, offset)[0] == 1

    @staticmethod
    def encode_byte(buf, offset, value):
        LE_INT8.pack_into(buf, offset, value)

    @staticmethod
    def decode_byte(buf, offset):
        return LE_INT8.unpack_from(buf, offset)[0]

    @staticmethod
    def encode_uuid(buf, offset, value):
        is_null = value is None
        FixSizedTypesCodec.encode_boolean(buf, offset, is_null)
        if is_null:
            return

        o = offset + BOOLEAN_SIZE_IN_BYTES
        LE_ULONG.pack_into(buf, o, value.int >> UUID_MSB_SHIFT)
        LE_ULONG.pack_into(buf, o + LONG_SIZE_IN_BYTES, value.int & UUID_LSB_MASK)

    @staticmethod
    def decode_uuid(buf, offset):
        is_null = FixSizedTypesCodec.decode_boolean(buf, offset)
        if is_null:
            return None

        msb_offset = offset + BOOLEAN_SIZE_IN_BYTES
        lsb_offset = msb_offset + LONG_SIZE_IN_BYTES
        b = (
            buf[lsb_offset - 1 : msb_offset - 1 : -1]
            + buf[lsb_offset + LONG_SIZE_IN_BYTES - 1 : lsb_offset - 1 : -1]
        )
        return uuid.UUID(bytes=bytes(b))

    @staticmethod
    def decode_short(buf, offset):
        return LE_INT16.unpack_from(buf, offset)[0]

    @staticmethod
    def decode_float(buf, offset):
        return LE_FLOAT.unpack_from(buf, offset)[0]

    @staticmethod
    def decode_double(buf, offset):
        return LE_DOUBLE.unpack_from(buf, offset)[0]

    @staticmethod
    def decode_local_date(buf, offset):
        year = FixSizedTypesCodec.decode_int(buf, offset)
        month = FixSizedTypesCodec.decode_byte(buf, offset + INT_SIZE_IN_BYTES)
        day = FixSizedTypesCodec.decode_byte(buf, offset + INT_SIZE_IN_BYTES + BYTE_SIZE_IN_BYTES)

        return date(year, month, day)

    @staticmethod
    def decode_local_time(buf, offset):
        hour = FixSizedTypesCodec.decode_byte(buf, offset)
        minute = FixSizedTypesCodec.decode_byte(buf, offset + BYTE_SIZE_IN_BYTES)
        second = FixSizedTypesCodec.decode_byte(buf, offset + BYTE_SIZE_IN_BYTES * 2)
        nano = FixSizedTypesCodec.decode_int(buf, offset + BYTE_SIZE_IN_BYTES * 3)

        return time(hour, minute, second, int(nano / 1000.0))

    @staticmethod
    def decode_local_date_time(buf, offset):
        date_value = FixSizedTypesCodec.decode_local_date(buf, offset)
        time_value = FixSizedTypesCodec.decode_local_time(buf, offset + _LOCAL_DATE_SIZE_IN_BYTES)

        return datetime.combine(date_value, time_value)

    @staticmethod
    def decode_offset_date_time(buf, offset):
        datetime_value = FixSizedTypesCodec.decode_local_date_time(buf, offset)
        offset_seconds = FixSizedTypesCodec.decode_int(buf, offset + _LOCAL_DATE_TIME_SIZE_IN_BYTES)

        return datetime_value.replace(tzinfo=timezone(timedelta(seconds=offset_seconds)))


class ListIntegerCodec(object):
    @staticmethod
    def encode(buf, arr, is_final=False):
        n = len(arr)
        size = SIZE_OF_FRAME_LENGTH_AND_FLAGS + n * INT_SIZE_IN_BYTES
        b = bytearray(size)
        LE_INT.pack_into(b, 0, size)
        if is_final:
            LE_UINT16.pack_into(b, INT_SIZE_IN_BYTES, _IS_FINAL_FLAG)
        for i in range(n):
            FixSizedTypesCodec.encode_int(
                b, SIZE_OF_FRAME_LENGTH_AND_FLAGS + i * INT_SIZE_IN_BYTES, arr[i]
            )
        buf.extend(b)

    @staticmethod
    def decode(msg):
        b = msg.next_frame().buf
        n = len(b) // INT_SIZE_IN_BYTES
        result = []
        for i in range(n):
            result.append(FixSizedTypesCodec.decode_int(b, i * INT_SIZE_IN_BYTES))
        return result


class ListLongCodec(object):
    @staticmethod
    def encode(buf, arr, is_final=False):
        n = len(arr)
        size = SIZE_OF_FRAME_LENGTH_AND_FLAGS + n * LONG_SIZE_IN_BYTES
        b = bytearray(size)
        LE_INT.pack_into(b, 0, size)
        if is_final:
            LE_UINT16.pack_into(b, INT_SIZE_IN_BYTES, _IS_FINAL_FLAG)
        for i in range(n):
            FixSizedTypesCodec.encode_long(
                b, SIZE_OF_FRAME_LENGTH_AND_FLAGS + i * LONG_SIZE_IN_BYTES, arr[i]
            )
        buf.extend(b)

    @staticmethod
    def decode(msg):
        b = msg.next_frame().buf
        n = len(b) // LONG_SIZE_IN_BYTES
        result = []
        for i in range(n):
            result.append(FixSizedTypesCodec.decode_long(b, i * LONG_SIZE_IN_BYTES))
        return result


class ListMultiFrameCodec(object):
    @staticmethod
    def encode(buf, arr, encoder, is_final=False):
        buf.extend(BEGIN_FRAME_BUF)
        for item in arr:
            encoder(buf, item)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def encode_contains_nullable(buf, arr, encoder, is_final=False):
        buf.extend(BEGIN_FRAME_BUF)
        for item in arr:
            if item is None:
                buf.extend(NULL_FRAME_BUF)
            else:
                encoder(buf, item)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def encode_nullable(buf, arr, encoder, is_final=False):
        if arr is None:
            if is_final:
                buf.extend(NULL_FINAL_FRAME_BUF)
            else:
                buf.extend(NULL_FRAME_BUF)
        else:
            ListMultiFrameCodec.encode(buf, arr, encoder, is_final)

    @staticmethod
    def decode(msg, decoder):
        result = []
        msg.next_frame()
        while not CodecUtil.next_frame_is_data_structure_end_frame(msg):
            result.append(decoder(msg))

        msg.next_frame()
        return result

    @staticmethod
    def decode_contains_nullable(msg, decoder):
        result = []
        msg.next_frame()
        while not CodecUtil.next_frame_is_data_structure_end_frame(msg):
            if CodecUtil.next_frame_is_null_frame(msg):
                result.append(None)
            else:
                result.append(decoder(msg))

        msg.next_frame()
        return result

    @staticmethod
    def decode_nullable(msg, decoder):
        if CodecUtil.next_frame_is_null_frame(msg):
            return None
        else:
            return ListMultiFrameCodec.decode(msg, decoder)


class ListUUIDCodec(object):
    @staticmethod
    def encode(buf, arr, is_final=False):
        n = len(arr)
        size = SIZE_OF_FRAME_LENGTH_AND_FLAGS + n * UUID_SIZE_IN_BYTES
        b = bytearray(size)
        LE_INT.pack_into(b, 0, size)
        if is_final:
            LE_UINT16.pack_into(b, INT_SIZE_IN_BYTES, _IS_FINAL_FLAG)
        for i in range(n):
            FixSizedTypesCodec.encode_uuid(
                b, SIZE_OF_FRAME_LENGTH_AND_FLAGS + i * UUID_SIZE_IN_BYTES, arr[i]
            )
        buf.extend(b)

    @staticmethod
    def decode(msg):
        b = msg.next_frame().buf
        n = len(b) // UUID_SIZE_IN_BYTES
        result = []
        for i in range(n):
            result.append(FixSizedTypesCodec.decode_uuid(b, i * UUID_SIZE_IN_BYTES))
        return result


class LongArrayCodec(object):
    @staticmethod
    def encode(buf, arr, is_final=False):
        n = len(arr)
        size = SIZE_OF_FRAME_LENGTH_AND_FLAGS + n * LONG_SIZE_IN_BYTES
        b = bytearray(size)
        LE_INT.pack_into(b, 0, size)
        if is_final:
            LE_UINT16.pack_into(b, INT_SIZE_IN_BYTES, _IS_FINAL_FLAG)
        for i in range(n):
            FixSizedTypesCodec.encode_long(
                b, SIZE_OF_FRAME_LENGTH_AND_FLAGS + i * LONG_SIZE_IN_BYTES, arr[i]
            )
        buf.extend(b)

    @staticmethod
    def decode(msg):
        b = msg.next_frame().buf
        n = len(b) // LONG_SIZE_IN_BYTES
        result = []
        for i in range(n):
            result.append(FixSizedTypesCodec.decode_long(b, i * LONG_SIZE_IN_BYTES))
        return result


class MapCodec(object):
    @staticmethod
    def encode(buf, m, key_encoder, value_encoder, is_final=False):
        buf.extend(BEGIN_FRAME_BUF)
        for key, value in m.items():
            key_encoder(buf, key)
            value_encoder(buf, value)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def encode_nullable(buf, m, key_encoder, value_encoder, is_final=False):
        if m is None:
            if is_final:
                buf.extend(NULL_FINAL_FRAME_BUF)
            else:
                buf.extend(NULL_FRAME_BUF)
        else:
            MapCodec.encode(buf, m, key_encoder, value_encoder, is_final)

    @staticmethod
    def decode(msg, key_decoder, value_decoder):
        result = dict()
        msg.next_frame()
        while not CodecUtil.next_frame_is_data_structure_end_frame(msg):
            key = key_decoder(msg)
            value = value_decoder(msg)
            result[key] = value

        msg.next_frame()
        return result

    @staticmethod
    def decode_nullable(msg, key_decoder, value_decoder):
        if CodecUtil.next_frame_is_null_frame(msg):
            return None
        else:
            return MapCodec.decode(msg, key_decoder, value_decoder)


class StringCodec(object):
    @staticmethod
    def encode(buf, value, is_final=False):
        value_bytes = value.encode("utf-8")
        header = bytearray(SIZE_OF_FRAME_LENGTH_AND_FLAGS)
        LE_INT.pack_into(header, 0, SIZE_OF_FRAME_LENGTH_AND_FLAGS + len(value_bytes))
        if is_final:
            LE_UINT16.pack_into(header, INT_SIZE_IN_BYTES, _IS_FINAL_FLAG)
        buf.extend(header)
        buf.extend(value_bytes)

    @staticmethod
    def decode(msg):
        return msg.next_frame().buf.decode("utf-8")


class ListCNFixedSizeCodec(object):
    _TYPE_NULL_ONLY = 1
    _TYPE_NOT_NULL_ONLY = 2
    _TYPE_MIXED = 3

    _ITEMS_PER_BITMASK = 8

    _HEADER_SIZE = BYTE_SIZE_IN_BYTES + INT_SIZE_IN_BYTES

    @staticmethod
    def decode(msg, item_size, decoder):
        frame = msg.next_frame()
        type = FixSizedTypesCodec.decode_byte(frame.buf, 0)
        count = FixSizedTypesCodec.decode_int(frame.buf, 1)

        if type == ListCNFixedSizeCodec._TYPE_NULL_ONLY:
            return [None] * count
        elif type == ListCNFixedSizeCodec._TYPE_NOT_NULL_ONLY:
            header_size = ListCNFixedSizeCodec._HEADER_SIZE
            return [decoder(frame.buf, header_size + i * item_size) for i in range(count)]
        else:
            response = [None] * count
            position = ListCNFixedSizeCodec._HEADER_SIZE
            read_count = 0
            items_per_bitmask = ListCNFixedSizeCodec._ITEMS_PER_BITMASK

            while read_count < count:
                bitmask = FixSizedTypesCodec.decode_byte(frame.buf, position)
                position += 1

                batch_size = min(items_per_bitmask, count - read_count)
                for i in range(batch_size):
                    mask = 1 << i
                    if (bitmask & mask) == mask:
                        response[read_count] = decoder(frame.buf, position)
                        position += item_size

                    read_count += 1

            return response


class ListCNBooleanCodec(object):
    @staticmethod
    def decode(msg):
        return ListCNFixedSizeCodec.decode(
            msg, BOOLEAN_SIZE_IN_BYTES, FixSizedTypesCodec.decode_boolean
        )


class ListCNByteCodec(object):
    @staticmethod
    def decode(msg):
        return ListCNFixedSizeCodec.decode(msg, BYTE_SIZE_IN_BYTES, FixSizedTypesCodec.decode_byte)


class ListCNShortCodec(object):
    @staticmethod
    def decode(msg):
        return ListCNFixedSizeCodec.decode(
            msg, SHORT_SIZE_IN_BYTES, FixSizedTypesCodec.decode_short
        )


class ListCNIntegerCodec(object):
    @staticmethod
    def decode(msg):
        return ListCNFixedSizeCodec.decode(msg, INT_SIZE_IN_BYTES, FixSizedTypesCodec.decode_int)


class ListCNLongCodec(object):
    @staticmethod
    def decode(msg):
        return ListCNFixedSizeCodec.decode(msg, LONG_SIZE_IN_BYTES, FixSizedTypesCodec.decode_long)


class ListCNFloatCodec(object):
    @staticmethod
    def decode(msg):
        return ListCNFixedSizeCodec.decode(
            msg, FLOAT_SIZE_IN_BYTES, FixSizedTypesCodec.decode_float
        )


class ListCNDoubleCodec(object):
    @staticmethod
    def decode(msg):
        return ListCNFixedSizeCodec.decode(
            msg, DOUBLE_SIZE_IN_BYTES, FixSizedTypesCodec.decode_double
        )


class ListCNLocalDateCodec(object):
    @staticmethod
    def decode(msg):
        return ListCNFixedSizeCodec.decode(
            msg, _LOCAL_DATE_SIZE_IN_BYTES, FixSizedTypesCodec.decode_local_date
        )


class ListCNLocalTimeCodec(object):
    @staticmethod
    def decode(msg):
        return ListCNFixedSizeCodec.decode(
            msg, _LOCAL_TIME_SIZE_IN_BYTES, FixSizedTypesCodec.decode_local_time
        )


class ListCNLocalDateTimeCodec(object):
    @staticmethod
    def decode(msg):
        return ListCNFixedSizeCodec.decode(
            msg, _LOCAL_DATE_TIME_SIZE_IN_BYTES, FixSizedTypesCodec.decode_local_date_time
        )


class ListCNOffsetDateTimeCodec(object):
    @staticmethod
    def decode(msg):
        return ListCNFixedSizeCodec.decode(
            msg, _OFFSET_DATE_TIME_SIZE_IN_BYTES, FixSizedTypesCodec.decode_offset_date_time
        )


class BigDecimalCodec(object):
    @staticmethod
    def decode(msg):
        buf = msg.next_frame().buf
        size = FixSizedTypesCodec.decode_int(buf, 0)
        unscaled_value = int_from_bytes(buf[INT_SIZE_IN_BYTES : INT_SIZE_IN_BYTES + size])
        scale = FixSizedTypesCodec.decode_int(buf, INT_SIZE_IN_BYTES + size)
        sign = 0 if unscaled_value >= 0 else 1
        return Decimal((sign, tuple(int(digit) for digit in str(abs(unscaled_value))), -1 * scale))


class SqlPageCodec(object):
    @staticmethod
    def decode(msg):
        from hazelcast.sql import SqlColumnType, _SqlPage

        # begin frame
        msg.next_frame()

        # read the "last" flag
        is_last = LE_INT8.unpack_from(msg.next_frame().buf, 0)[0] == 1

        # read column types
        column_type_ids = ListIntegerCodec.decode(msg)
        column_count = len(column_type_ids)

        # read columns
        columns = [None] * column_count

        for i in range(column_count):
            column_type_id = column_type_ids[i]

            if column_type_id == SqlColumnType.VARCHAR:
                columns[i] = ListMultiFrameCodec.decode_contains_nullable(msg, StringCodec.decode)
            elif column_type_id == SqlColumnType.BOOLEAN:
                columns[i] = ListCNBooleanCodec.decode(msg)
            elif column_type_id == SqlColumnType.TINYINT:
                columns[i] = ListCNByteCodec.decode(msg)
            elif column_type_id == SqlColumnType.SMALLINT:
                columns[i] = ListCNShortCodec.decode(msg)
            elif column_type_id == SqlColumnType.INTEGER:
                columns[i] = ListCNIntegerCodec.decode(msg)
            elif column_type_id == SqlColumnType.BIGINT:
                columns[i] = ListCNLongCodec.decode(msg)
            elif column_type_id == SqlColumnType.REAL:
                columns[i] = ListCNFloatCodec.decode(msg)
            elif column_type_id == SqlColumnType.DOUBLE:
                columns[i] = ListCNDoubleCodec.decode(msg)
            elif column_type_id == SqlColumnType.DATE:
                columns[i] = ListCNLocalDateCodec.decode(msg)
            elif column_type_id == SqlColumnType.TIME:
                columns[i] = ListCNLocalTimeCodec.decode(msg)
            elif column_type_id == SqlColumnType.TIMESTAMP:
                columns[i] = ListCNLocalDateTimeCodec.decode(msg)
            elif column_type_id == SqlColumnType.TIMESTAMP_WITH_TIME_ZONE:
                columns[i] = ListCNOffsetDateTimeCodec.decode(msg)
            elif column_type_id == SqlColumnType.DECIMAL:
                columns[i] = ListMultiFrameCodec.decode_contains_nullable(
                    msg, BigDecimalCodec.decode
                )
            elif column_type_id == SqlColumnType.NULL:
                frame = msg.next_frame()
                size = FixSizedTypesCodec.decode_int(frame.buf, 0)
                column = [None for _ in range(size)]
                columns[i] = column
            elif column_type_id == SqlColumnType.OBJECT:
                columns[i] = ListMultiFrameCodec.decode_contains_nullable(msg, DataCodec.decode)
            else:
                raise ValueError("Unknown type %s" % column_type_id)

        CodecUtil.fast_forward_to_end_frame(msg)

        return _SqlPage(column_type_ids, columns, is_last)
