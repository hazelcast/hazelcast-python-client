# coding: utf-8
import unittest

from hazelcast.protocol.client_message import *
from hazelcast.protocol.codec import client_authentication_codec

READ_HEADER = "00" * 20 + "1600"


class ClientMessageTest(unittest.TestCase):
    def test_header_fields(self):
        message = ClientMessage(payload_size=30)

        correlation_id = 6474838
        message_type = 987
        flags = 5
        partition_id = 27
        frame_length = 100
        data_offset = 17

        message.set_correlation_id(correlation_id)
        message.set_message_type(message_type)
        message.set_flags(flags)
        message.set_partition_id(partition_id)
        message.set_frame_length(frame_length)
        message.set_data_offset(data_offset)

        self.assertEqual(correlation_id, message.get_correlation_id())
        self.assertEqual(message_type, message.get_message_type())
        self.assertEqual(flags, message.get_flags())
        self.assertEqual(partition_id, message.get_partition_id())
        self.assertEqual(frame_length, message.get_frame_length())
        self.assertEqual(data_offset, message.get_data_offset())

    def test_append_byte(self):
        message = ClientMessage(payload_size=30)

        message.append_byte(0x21)
        message.append_byte(0xF2)
        message.append_byte(0x34)

        data_offset = message.get_data_offset()
        self.assertEqual(b"21f234", binascii.hexlify(message.buffer[data_offset:data_offset + 3]))

    def test_append_bool(self):
        message = ClientMessage(payload_size=30)

        message.append_bool(True)

        data_offset = message.get_data_offset()
        self.assertEqual(b"01", binascii.hexlify(message.buffer[data_offset:data_offset + 1]))

    def test_append_int(self):
        message = ClientMessage(payload_size=30)

        message.append_int(0x1feeddcc)

        data_offset = message.get_data_offset()
        self.assertEqual(b"ccddee1f", binascii.hexlify(message.buffer[data_offset:data_offset + 4]))

    def test_append_long(self):
        message = ClientMessage(payload_size=30)

        message.append_long(0x1feeddccbbaa8765)

        data_offset = message.get_data_offset()
        self.assertEqual(b"6587aabbccddee1f", binascii.hexlify(message.buffer[data_offset:data_offset + 8]))

    def test_append_str(self):
        message = ClientMessage(payload_size=30)

        frame_length = 1
        flags = 2
        message_type = 3
        correlation_id = 4
        partition_id = 5

        message.set_correlation_id(correlation_id)
        message.set_message_type(message_type)
        message.set_flags(flags)
        message.set_partition_id(partition_id)
        message.set_frame_length(frame_length)

        message.append_str("abc")

        # buffer content should be
        # 01000000 00 02 0300 0400000000000000 05000000 1600 03000000 616263 0000000000000000000000000000000000000000000000
        self.assertEqual(b"01000000", binascii.hexlify(message.buffer[0:4]))
        self.assertEqual(b"00", binascii.hexlify(message.buffer[4:5]))
        self.assertEqual(b"02", binascii.hexlify(message.buffer[5:6]))
        self.assertEqual(b"0300", binascii.hexlify(message.buffer[6:8]))
        self.assertEqual(b"0400000000000000", binascii.hexlify(message.buffer[8:16]))
        self.assertEqual(b"05000000", binascii.hexlify(message.buffer[16:20]))
        self.assertEqual(b"1600", binascii.hexlify(message.buffer[20:22]))
        self.assertEqual(b"03000000", binascii.hexlify(message.buffer[22:26]))
        self.assertEqual(b"616263", binascii.hexlify(message.buffer[26:29]))

    def test_read_byte(self):
        hexstr = READ_HEADER + "78"
        buf = binascii.unhexlify(hexstr)

        message = ClientMessage(buff=buf)
        self.assertEqual(0x78, message.read_byte())

    def test_read_bool(self):
        hexstr = READ_HEADER + "01"
        buf = binascii.unhexlify(hexstr)

        message = ClientMessage(buff=buf)
        self.assertEqual(True, message.read_bool())

    def test_read_int(self):
        hexstr = READ_HEADER + "12345678"
        buf = binascii.unhexlify(hexstr)

        message = ClientMessage(buff=buf)
        self.assertEqual(0x78563412, message.read_int())

    def test_read_long(self):
        hexstr = READ_HEADER + "6587aabbccddee1f"
        buf = binascii.unhexlify(hexstr)

        message = ClientMessage(buff=buf)
        self.assertEqual(0x1feeddccbbaa8765, message.read_long())

    def test_read_str(self):
        hexstr = READ_HEADER + "03000000616263"
        buf = binascii.unhexlify(hexstr)

        message = ClientMessage(buff=buf)
        self.assertEqual("abc", message.read_str())

    def test_no_flag(self):
        message = ClientMessage(payload_size=30)
        message.set_flags(0)

        self.assertFalse(message.is_flag_set(BEGIN_FLAG))
        self.assertFalse(message.is_flag_set(END_FLAG))
        self.assertFalse(message.is_flag_set(LISTENER_FLAG))

    def test_set_flag_begin(self):
        message = ClientMessage(payload_size=30)
        message.set_flags(0)

        message.add_flag(BEGIN_FLAG)

        self.assertTrue(message.is_flag_set(BEGIN_FLAG))
        self.assertFalse(message.is_flag_set(END_FLAG))
        self.assertFalse(message.is_flag_set(LISTENER_FLAG))

    def test_set_flag_end(self):
        message = ClientMessage(payload_size=30)
        message.set_flags(0)

        message.add_flag(END_FLAG)

        self.assertFalse(message.is_flag_set(BEGIN_FLAG))
        self.assertTrue(message.is_flag_set(END_FLAG))
        self.assertFalse(message.is_flag_set(LISTENER_FLAG))

    def test_set_flag_listener(self):
        message = ClientMessage(payload_size=30)
        message.set_flags(0)

        message.add_flag(LISTENER_FLAG)

        self.assertFalse(message.is_flag_set(BEGIN_FLAG))
        self.assertFalse(message.is_flag_set(END_FLAG))
        self.assertTrue(message.is_flag_set(LISTENER_FLAG))

    def test_clone(self):
        message = ClientMessage(payload_size=0)
        message.set_flags(0)

        message.add_flag(LISTENER_FLAG)
        clone = message.clone()
        clone.add_flag(BEGIN_FLAG)

        self.assertTrue(message.is_flag_set(LISTENER_FLAG))
        self.assertTrue(clone.is_flag_set(LISTENER_FLAG))
        self.assertFalse(message.is_flag_set(BEGIN_FLAG))
        self.assertTrue(clone.is_flag_set(BEGIN_FLAG))


class ClientMessageBuilderTest(unittest.TestCase):
    def test_message_accumulate(self):
        message = client_authentication_codec.encode_request("user", "pass", "uuid", "owner-uuid", True, "PYH", 1, "3.10")
        message.set_correlation_id(1)

        def message_callback(merged_message):
            self.assertTrue(merged_message.is_flag_set(BEGIN_END_FLAG))
            self.assertEqual(merged_message.get_frame_length(), message.get_frame_length())
            self.assertEqual(merged_message.get_correlation_id(), message.get_correlation_id())

        builder = ClientMessageBuilder(message_callback=message_callback)

        header = message.buffer[0:message.get_data_offset()]
        payload = message.buffer[message.get_data_offset():]

        indx_1 = len(payload) // 3
        indx_2 = 2 * len(payload) // 3

        p1 = payload[0:indx_1]
        p2 = payload[indx_1: indx_2]
        p3 = payload[indx_2:]

        cm1 = ClientMessage(buff=header + p1)
        cm2 = ClientMessage(buff=header + p2)
        cm3 = ClientMessage(buff=header + p3)
        cm1.add_flag(BEGIN_FLAG)
        cm3.add_flag(END_FLAG)

        builder.on_message(cm1)
        builder.on_message(cm2)
        builder.on_message(cm3)

