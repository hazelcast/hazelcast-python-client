# coding: utf-8
import unittest

from hazelcast.client_message import *

READ_HEADER = "00" * 16 + "1200"


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

        self.assertEqual("21f234", binascii.hexlify(message._buffer[18:21]))

    def test_append_bool(self):
        message = ClientMessage(payload_size=30)

        message.append_bool(True)

        self.assertEqual("01", binascii.hexlify(message._buffer[18:19]))

    def test_append_int(self):
        message = ClientMessage(payload_size=30)

        message.append_int(0x1feeddcc)

        self.assertEqual("ccddee1f", binascii.hexlify(message._buffer[18:22]))

    def test_append_long(self):
        message = ClientMessage(payload_size=30)

        message.append_long(0x1feeddccbbaa8765)

        self.assertEqual("6587aabbccddee1f", binascii.hexlify(message._buffer[18:26]))

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
        # 01000000 00 02 0300 04000000 05000000 1200 03000000 616263 0000000000000000000000000000000000000000000000
        self.assertEqual("01000000", binascii.hexlify(message._buffer[0:4]))
        self.assertEqual("00", binascii.hexlify(message._buffer[4:5]))
        self.assertEqual("02", binascii.hexlify(message._buffer[5:6]))
        self.assertEqual("0300", binascii.hexlify(message._buffer[6:8]))
        self.assertEqual("04000000", binascii.hexlify(message._buffer[8:12]))
        self.assertEqual("05000000", binascii.hexlify(message._buffer[12:16]))
        self.assertEqual("1200", binascii.hexlify(message._buffer[16:18]))
        self.assertEqual("03000000", binascii.hexlify(message._buffer[18:22]))
        self.assertEqual("616263", binascii.hexlify(message._buffer[22:25]))

        print message

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


if __name__ == '__main__':
    unittest.main()
