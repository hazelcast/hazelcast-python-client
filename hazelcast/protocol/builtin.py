import uuid

from hazelcast import six
from hazelcast.six.moves import range
from hazelcast.protocol.client_message import (
    NULL_FRAME_BUF,
    BEGIN_FRAME_BUF,
    END_FRAME_BUF,
    SIZE_OF_FRAME_LENGTH_AND_FLAGS,
    _IS_FINAL_FLAG,
    NULL_FINAL_FRAME_BUF,
    END_FINAL_FRAME_BUF,
)
from hazelcast.serialization import (
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
)
from hazelcast.serialization.data import Data


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
        for key, value in six.iteritems(m):
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
