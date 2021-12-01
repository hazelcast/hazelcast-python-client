# coding: utf-8
import unittest
import uuid

from hazelcast.connection import _Reader
from hazelcast.errors import _ErrorsCodec
from hazelcast.protocol import ErrorHolder
from hazelcast.protocol.builtin import (
    CodecUtil,
    FixSizedTypesCodec,
    ByteArrayCodec,
    DataCodec,
    EntryListCodec,
    StringCodec,
    EntryListUUIDListIntegerCodec,
    EntryListUUIDLongCodec,
    ListMultiFrameCodec,
    ListIntegerCodec,
    ListLongCodec,
    ListUUIDCodec,
    MapCodec,
)
from hazelcast.protocol.client_message import *
from hazelcast.protocol.codec import client_authentication_codec
from hazelcast.protocol.codec.custom.error_holder_codec import ErrorHolderCodec
from hazelcast.serialization.data import Data


class OutboundMessageTest(unittest.TestCase):
    def test_header_fields(self):
        # 6 bytes for the length + flags + 4 bytes message type + 8 bytes correlation id + 4 bytes partition id
        buf = bytearray(22)
        message = OutboundMessage(buf, False)
        self.assertFalse(message.retryable)
        message.set_correlation_id(42)
        message.set_partition_id(23)

        correlation_id = LE_LONG.unpack_from(message.buf, 6 + 4)[0]
        partition_id = LE_INT.unpack_from(message.buf, 6 + 4 + 8)[0]
        self.assertEqual(42, correlation_id)
        self.assertEqual(42, message.get_correlation_id())
        self.assertEqual(23, partition_id)

    def test_copy(self):
        buf = bytearray(range(20))
        message = OutboundMessage(buf, True)

        copy = message.copy()
        self.assertTrue(copy.retryable)
        buf[0] = 99
        self.assertEqual(99, message.buf[0])
        self.assertEqual(0, copy.buf[0])  # should be a deep copy

    def test_encode(self):
        msg = client_authentication_codec.encode_request(
            "dev",
            "username",
            "password",
            uuid.UUID(hex="1862c7d2-f89c-4151-981d-07a6287089d3"),
            "PYH",
            1,
            "5.0",
            "hz.client_0",
            ["label"],
        )

        # fmt: off
        expected = [
            # initial frame
            40, 0, 0, 0,  # length
            0, 192,  # flags
            0, 1, 0, 0,  # message type
            0, 0, 0, 0, 0, 0, 0, 0,  # correlation id
            255, 255, 255, 255,  # partition id
            0, 81, 65, 156, 248, 210, 199, 98, 24, 211, 137, 112, 40, 166, 7, 29, 152,  # uuid
            1,  # serialization version

            # cluster name frame
            9, 0, 0, 0,  # length
            0, 0,  # flags
            100, 101, 118,  # cluster name

            # username frame
            14, 0, 0, 0,  # length
            0, 0,  # length
            117, 115, 101, 114, 110, 97, 109, 101,  # username

            # password frame
            14, 0, 0, 0,  # length
            0, 0,  # flags
            112, 97, 115, 115, 119, 111, 114, 100,  # password

            # client type frame
            9, 0, 0, 0,  # length
            0, 0,  # flags
            80, 89, 72,  # client type

            # client version frame
            9, 0, 0, 0,  # length
            0, 0,  # flags
            53, 46, 48,  # version

            # client name frame
            17, 0, 0, 0,  # length
            0, 0,  # flags
            104, 122, 46, 99, 108, 105, 101, 110, 116, 95, 48,  # client name

            # labels begin frame
            6, 0, 0, 0,  # length
            0, 16,  # flags

            # labels[0] frame
            11, 0, 0, 0,  # length
            0, 0,  # flags
            108, 97, 98, 101, 108,  # labels[0]

            # labels end frame
            6, 0, 0, 0,  # length
            0, 40,  # flags
        ]
        # fmt: on

        self.assertEqual(bytearray(expected), msg.buf)


BEGIN_FRAME = Frame(bytearray(0), 1 << 12)
END_FRAME = Frame(bytearray(), 1 << 11)


class InboundMessageTest(unittest.TestCase):
    def test_fast_forward(self):
        message = InboundMessage(BEGIN_FRAME.copy())

        # New custom-typed parameter with its own begin and end frames
        message.add_frame(BEGIN_FRAME.copy())
        message.add_frame(Frame(bytearray(0), 0))
        message.add_frame(END_FRAME.copy())

        message.add_frame(END_FRAME.copy())

        # begin frame
        message.next_frame()
        CodecUtil.fast_forward_to_end_frame(message)
        self.assertFalse(message.has_next_frame())


class EncodeDecodeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.reader = _Reader(None)

    def setUp(self):
        self.buf = create_initial_buffer(50, 0, True)
        self.message = OutboundMessage(self.buf, False)

    def write_and_decode(self):
        self.reader.read(self.message.buf)
        return self.reader._read_message()

    def mark_initial_frame_as_non_final(self):
        flags = 1 << 11 | 1 << 12
        LE_UINT16.pack_into(self.buf, INT_SIZE_IN_BYTES, flags)

    def test_byte(self):
        FixSizedTypesCodec.encode_byte(self.buf, 16, 3)
        message = self.write_and_decode()
        buf = message.next_frame().buf
        self.assertEqual(3, FixSizedTypesCodec.decode_byte(buf, 10))

    def test_boolean(self):
        FixSizedTypesCodec.encode_boolean(self.buf, 16, True)
        message = self.write_and_decode()
        buf = message.next_frame().buf
        self.assertEqual(True, FixSizedTypesCodec.decode_boolean(buf, 10))

    def test_int(self):
        FixSizedTypesCodec.encode_int(self.buf, 16, 1234)
        message = self.write_and_decode()
        buf = message.next_frame().buf
        self.assertEqual(1234, FixSizedTypesCodec.decode_int(buf, 10))

    def test_uuid(self):
        random_uuid = uuid.uuid4()
        FixSizedTypesCodec.encode_uuid(self.buf, 16, random_uuid)
        message = self.write_and_decode()
        buf = message.next_frame().buf
        self.assertEqual(random_uuid, FixSizedTypesCodec.decode_uuid(buf, 10))

    def test_none_uuid(self):
        FixSizedTypesCodec.encode_uuid(self.buf, 16, None)
        message = self.write_and_decode()
        buf = message.next_frame().buf
        self.assertIsNone(FixSizedTypesCodec.decode_uuid(buf, 10))

    def test_long(self):
        FixSizedTypesCodec.encode_long(self.buf, 16, 1234567890123)
        message = self.write_and_decode()
        buf = message.next_frame().buf
        self.assertEqual(1234567890123, FixSizedTypesCodec.decode_long(buf, 10))

    def test_byte_array(self):
        self.mark_initial_frame_as_non_final()
        b = "abc©☺𩸽".encode("utf-8")
        ByteArrayCodec.encode(self.buf, b, True)
        message = self.write_and_decode()
        message.next_frame()  # initial frame
        self.assertEqual(b, ByteArrayCodec.decode(message))

    def test_data(self):
        self.mark_initial_frame_as_non_final()
        data = Data("123456789".encode("utf-8"))
        DataCodec.encode(self.buf, data)
        DataCodec.encode_nullable(self.buf, data)
        DataCodec.encode_nullable(self.buf, None, True)
        message = self.write_and_decode()
        message.next_frame()  # initial frame
        self.assertEqual(data, DataCodec.decode(message))
        self.assertEqual(data, DataCodec.decode_nullable(message))
        self.assertIsNone(DataCodec.decode_nullable(message))

    def test_entry_list(self):
        self.mark_initial_frame_as_non_final()
        entries = [("a", "1"), ("b", "2"), ("c", "3")]
        EntryListCodec.encode(self.buf, entries, StringCodec.encode, StringCodec.encode)
        EntryListCodec.encode_nullable(self.buf, entries, StringCodec.encode, StringCodec.encode)
        EntryListCodec.encode_nullable(self.buf, None, StringCodec.encode, StringCodec.encode, True)
        message = self.write_and_decode()
        message.next_frame()  # initial frame
        self.assertEqual(
            entries, EntryListCodec.decode(message, StringCodec.decode, StringCodec.decode)
        )
        self.assertEqual(
            entries, EntryListCodec.decode_nullable(message, StringCodec.decode, StringCodec.decode)
        )
        self.assertIsNone(
            EntryListCodec.decode_nullable(message, StringCodec.decode, StringCodec.decode)
        )

    def test_uuid_integer_list_entry_list(self):
        self.mark_initial_frame_as_non_final()
        entries = [(uuid.uuid4(), [1, 2]), (uuid.uuid4(), [3, 4]), (uuid.uuid4(), [5, 6])]
        EntryListUUIDListIntegerCodec.encode(self.buf, entries, True)
        message = self.write_and_decode()
        message.next_frame()  # initial frame
        self.assertEqual(entries, EntryListUUIDListIntegerCodec.decode(message))

    def test_uuid_long_entry_list(self):
        self.mark_initial_frame_as_non_final()
        entries = [(uuid.uuid4(), 0xCAFE), (uuid.uuid4(), 0xBABE), (uuid.uuid4(), 56789123123123)]
        EntryListUUIDLongCodec.encode(self.buf, entries, True)
        message = self.write_and_decode()
        message.next_frame()  # initial frame
        self.assertEqual(entries, EntryListUUIDLongCodec.decode(message))

    def test_errors(self):
        self.mark_initial_frame_as_non_final()
        holder = ErrorHolder(-12345, "class", "message", [])
        ListMultiFrameCodec.encode(self.buf, [holder], ErrorHolderCodec.encode, True)
        message = self.write_and_decode()
        self.assertEqual([holder], _ErrorsCodec.decode(message))

    def test_integer_list(self):
        self.mark_initial_frame_as_non_final()
        l = [0xCAFE, 0xBABE, -9999999]
        ListIntegerCodec.encode(self.buf, l, True)
        message = self.write_and_decode()
        message.next_frame()  # initial frame
        self.assertEqual(l, ListIntegerCodec.decode(message))

    def test_long_list(self):
        self.mark_initial_frame_as_non_final()
        l = [1, -2, 56789123123123]
        ListLongCodec.encode(self.buf, l, True)
        message = self.write_and_decode()
        message.next_frame()  # initial frame
        self.assertEqual(l, ListLongCodec.decode(message))

    def test_list(self):
        self.mark_initial_frame_as_non_final()
        l = ["a", "b", "c", "😃"]
        ListMultiFrameCodec.encode(self.buf, l, StringCodec.encode)
        ListMultiFrameCodec.encode_nullable(self.buf, l, StringCodec.encode)
        ListMultiFrameCodec.encode_nullable(self.buf, None, StringCodec.encode)
        ListMultiFrameCodec.encode_contains_nullable(self.buf, l, StringCodec.encode)
        ListMultiFrameCodec.encode_contains_nullable(self.buf, [None], StringCodec.encode, True)
        message = self.write_and_decode()
        message.next_frame()  # initial frame
        self.assertEqual(l, ListMultiFrameCodec.decode(message, StringCodec.decode))
        self.assertEqual(l, ListMultiFrameCodec.decode_nullable(message, StringCodec.decode))
        self.assertIsNone(ListMultiFrameCodec.decode_nullable(message, StringCodec.decode))
        self.assertEqual(
            l, ListMultiFrameCodec.decode_contains_nullable(message, StringCodec.decode)
        )
        self.assertEqual(
            [None], ListMultiFrameCodec.decode_contains_nullable(message, StringCodec.decode)
        )

    def test_uuid_list(self):
        self.mark_initial_frame_as_non_final()
        l = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]
        ListUUIDCodec.encode(self.buf, l, True)
        message = self.write_and_decode()
        message.next_frame()  # initial frame
        self.assertEqual(l, ListUUIDCodec.decode(message))

    def test_map(self):
        self.mark_initial_frame_as_non_final()
        m = dict()
        m["a"] = "b"
        m["c"] = "d"
        m["e"] = "f"
        MapCodec.encode(self.buf, m, StringCodec.encode, StringCodec.encode)
        MapCodec.encode_nullable(self.buf, m, StringCodec.encode, StringCodec.encode)
        MapCodec.encode_nullable(self.buf, None, StringCodec.encode, StringCodec.encode, True)
        message = self.write_and_decode()
        message.next_frame()  # initial frame
        self.assertEqual(m, MapCodec.decode(message, StringCodec.decode, StringCodec.decode))
        self.assertEqual(
            m, MapCodec.decode_nullable(message, StringCodec.decode, StringCodec.decode)
        )
        self.assertIsNone(MapCodec.decode_nullable(message, StringCodec.decode, StringCodec.decode))

    def test_string(self):
        self.mark_initial_frame_as_non_final()
        string = "abc©☺𩸽🐶😁"
        StringCodec.encode(self.buf, string, True)
        message = self.write_and_decode()
        message.next_frame()  # initial frame
        self.assertEqual(string, StringCodec.decode(message))

    def test_nullable(self):
        self.mark_initial_frame_as_non_final()
        CodecUtil.encode_nullable(self.buf, "a", StringCodec.encode)
        CodecUtil.encode_nullable(self.buf, None, StringCodec.encode, True)
        message = self.write_and_decode()
        message.next_frame()  # initial frame
        self.assertEqual("a", CodecUtil.decode_nullable(message, StringCodec.decode))
        self.assertIsNone(CodecUtil.decode_nullable(message, StringCodec.decode))


class _MutableInteger(object):
    def __init__(self, initial_value):
        self.value = initial_value

    def increment(self):
        self.value += 1


class ClientMessageBuilderTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.reader = _Reader(None)

    def setUp(self):
        self.counter = _MutableInteger(0)
        self.builder = ClientMessageBuilder(lambda m: self.counter.increment())

    def test_unfragmented_message(self):
        request = client_authentication_codec.encode_request(
            "dev", "user", "pass", uuid.uuid4(), "PYH", 1, "4.0", "python", []
        )
        self.reader.read(request.buf)
        message = self.reader._read_message()
        self.builder.on_message(message)
        self.assertEqual(1, self.counter.value)

    def test_fragmented_message(self):
        size = SIZE_OF_FRAME_LENGTH_AND_FLAGS + LONG_SIZE_IN_BYTES
        fragmentation_id = 1234567890123
        begin_buf = bytearray(size)
        LE_INT.pack_into(begin_buf, 0, size)
        LE_UINT16.pack_into(begin_buf, INT_SIZE_IN_BYTES, 1 << 15)
        LE_LONG.pack_into(begin_buf, SIZE_OF_FRAME_LENGTH_AND_FLAGS, fragmentation_id)
        StringCodec.encode(begin_buf, "a", True)

        middle_buf = bytearray(size)
        LE_INT.pack_into(middle_buf, 0, size)
        LE_LONG.pack_into(middle_buf, SIZE_OF_FRAME_LENGTH_AND_FLAGS, fragmentation_id)
        StringCodec.encode(middle_buf, "b", True)

        end_buf = bytearray(size)
        LE_INT.pack_into(end_buf, 0, size)
        LE_UINT16.pack_into(end_buf, INT_SIZE_IN_BYTES, 1 << 14)
        LE_LONG.pack_into(end_buf, SIZE_OF_FRAME_LENGTH_AND_FLAGS, fragmentation_id)
        StringCodec.encode(end_buf, "c", True)

        self.reader.read(begin_buf)
        begin_message = self.reader._read_message()
        self.builder.on_message(begin_message)
        self.assertEqual(0, self.counter.value)
        self.assertEqual(1, len(self.builder._fragmented_messages))
        fragmented_message = self.builder._fragmented_messages[fragmentation_id]
        self.assertIsNotNone(fragmented_message)
        self.assertEqual("a", fragmented_message.end_frame.buf.decode("utf-8"))

        self.reader.read(middle_buf)
        middle_message = self.reader._read_message()
        self.builder.on_message(middle_message)
        self.assertEqual(0, self.counter.value)
        self.assertEqual(1, len(self.builder._fragmented_messages))
        fragmented_message = self.builder._fragmented_messages[fragmentation_id]
        self.assertIsNotNone(fragmented_message)
        self.assertEqual("b", fragmented_message.end_frame.buf.decode("utf-8"))

        self.reader.read(end_buf)
        end_message = self.reader._read_message()
        self.builder.on_message(end_message)
        self.assertEqual(1, self.counter.value)
        self.assertEqual(0, len(self.builder._fragmented_messages))
