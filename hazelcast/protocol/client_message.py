"""
Client Message is the carrier framed data as defined below.
Any request parameter, response or event data will be carried in the payload.

0                   1                   2                   3
0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|R|                      Frame Length                           |
+-------------+---------------+---------------------------------+
|  Version    |B|E|  Flags    |               Type              |
+-------------+---------------+---------------------------------+
|                                                               |
+                       CorrelationId                           +
|                                                               |
+---------------------------------------------------------------+
|                        PartitionId                            |
+-----------------------------+---------------------------------+
|        Data Offset          |                                 |
+-----------------------------+                                 |
|                      Message Payload Data                    ...
|                                                              ...


"""
import binascii
import logging
import struct

from hazelcast.serialization.data import *

# constants
VERSION = 0
BEGIN_FLAG = 0x80
END_FLAG = 0x40
BEGIN_END_FLAG = BEGIN_FLAG | END_FLAG
LISTENER_FLAG = 0x01

PAYLOAD_OFFSET = 18
SIZE_OFFSET = 0

FRAME_LENGTH_FIELD_OFFSET = 0
VERSION_FIELD_OFFSET = FRAME_LENGTH_FIELD_OFFSET + INT_SIZE_IN_BYTES
FLAGS_FIELD_OFFSET = VERSION_FIELD_OFFSET + BYTE_SIZE_IN_BYTES
TYPE_FIELD_OFFSET = FLAGS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES
CORRELATION_ID_FIELD_OFFSET = TYPE_FIELD_OFFSET + SHORT_SIZE_IN_BYTES
PARTITION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES
DATA_OFFSET_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES
HEADER_SIZE = DATA_OFFSET_FIELD_OFFSET + SHORT_SIZE_IN_BYTES


class ClientMessage(object):
    def __init__(self, buff=None, payload_size=0):
        if buff:
            self.buffer = buff
            self._read_index = 0
        else:
            self.buffer = bytearray(HEADER_SIZE + payload_size)
            self.set_data_offset(HEADER_SIZE)
            self._write_index = 0
        self._retryable = False

    # HEADER ACCESSORS
    def get_correlation_id(self):
        return struct.unpack_from(FMT_LE_LONG, self.buffer, CORRELATION_ID_FIELD_OFFSET)[0]

    def set_correlation_id(self, val):
        struct.pack_into(FMT_LE_LONG, self.buffer, CORRELATION_ID_FIELD_OFFSET, val)
        return self

    def get_partition_id(self):
        return struct.unpack_from(FMT_LE_INT, self.buffer, PARTITION_ID_FIELD_OFFSET)[0]

    def set_partition_id(self, val):
        struct.pack_into(FMT_LE_INT, self.buffer, PARTITION_ID_FIELD_OFFSET, val)
        return self

    def get_message_type(self):
        return struct.unpack_from(FMT_LE_UINT16, self.buffer, TYPE_FIELD_OFFSET)[0]

    def set_message_type(self, val):
        struct.pack_into(FMT_LE_UINT16, self.buffer, TYPE_FIELD_OFFSET, val)
        return self

    def get_flags(self):
        return struct.unpack_from(FMT_LE_UINT8, self.buffer, FLAGS_FIELD_OFFSET)[0]

    def set_flags(self, val):
        struct.pack_into(FMT_LE_UINT8, self.buffer, FLAGS_FIELD_OFFSET, val)
        return self

    def has_flags(self, flags):
        return self.get_flags() & flags

    def get_frame_length(self):
        return struct.unpack_from(FMT_LE_INT, self.buffer, FRAME_LENGTH_FIELD_OFFSET)[0]

    def set_frame_length(self, val):
        struct.pack_into(FMT_LE_INT, self.buffer, FRAME_LENGTH_FIELD_OFFSET, val)
        return self

    def get_data_offset(self):
        return struct.unpack_from(FMT_LE_UINT16, self.buffer, DATA_OFFSET_FIELD_OFFSET)[0]

    def set_data_offset(self, val):
        struct.pack_into(FMT_LE_UINT16, self.buffer, DATA_OFFSET_FIELD_OFFSET, val)
        return self

    def _write_offset(self):
        return self.get_data_offset() + self._write_index

    def _read_offset(self):
        return self.get_data_offset() + self._read_index

    # PAYLOAD
    def append_byte(self, val):
        struct.pack_into(FMT_LE_UINT8, self.buffer, self._write_offset(), val)
        self._write_index += BYTE_SIZE_IN_BYTES
        return self

    def append_bool(self, val):
        return self.append_byte(1 if val else 0)

    def append_int(self, val):
        struct.pack_into(FMT_LE_INT, self.buffer, self._write_offset(), val)
        self._write_index += INT_SIZE_IN_BYTES
        return self

    def append_long(self, val):
        struct.pack_into(FMT_LE_LONG, self.buffer, self._write_offset(), val)
        self._write_index += LONG_SIZE_IN_BYTES
        return self

    def append_str(self, val):
        self.append_byte_array(val.encode("utf-8"))
        return self

    def append_data(self, val):
        self.append_byte_array(val.to_bytes())
        return self

    def append_byte_array(self, arr):
        length = len(arr)
        # length
        self.append_int(length)
        # copy content
        self.buffer[self._write_offset(): self._write_offset() + length] = arr[:]
        self._write_index += length

    def append_tuple(self, entry_tuple):
        self.append_data(entry_tuple[0]).append_data(entry_tuple[1])
        return self

    # PAYLOAD READ
    def _read_from_buff(self, fmt, size):
        val = struct.unpack_from(fmt, self.buffer, self._read_offset())
        self._read_index += size
        return val[0]

    def read_byte(self):
        return self._read_from_buff(FMT_LE_UINT8, BYTE_SIZE_IN_BYTES)

    def read_bool(self):
        return True if self.read_byte() else False

    def read_int(self):
        return self._read_from_buff(FMT_LE_INT, INT_SIZE_IN_BYTES)

    def read_long(self):
        return self._read_from_buff(FMT_LE_LONG, LONG_SIZE_IN_BYTES)

    def read_str(self):
        return self.read_byte_array().decode("utf-8")

    def read_data(self):
        return Data(self.read_byte_array())

    def read_byte_array(self):
        length = self.read_int()
        result = bytearray(self.buffer[self._read_offset(): self._read_offset() + length])
        self._read_index += length
        return result

    # helpers

    def is_retryable(self):
        return self._retryable

    def set_retryable(self, val):
        self._retryable = val
        return self

    def is_complete(self):
        try:
            return (self._read_offset() >= HEADER_SIZE) and (self._read_offset() == self.get_frame_length())
        except AttributeError:
            return False

    def is_flag_set(self, flag):
        i = self.get_flags() & flag
        return i == flag

    def add_flag(self, flags):
        self.set_flags(self.get_flags() | flags)
        return self

    def update_frame_length(self):
        self.set_frame_length(self._write_offset())
        return self

    def accumulate(self, client_message):
        start = client_message.get_data_offset()
        end = client_message.get_frame_length()
        self.buffer += client_message.buffer[start:end]
        self.set_frame_length(len(self.buffer))

    def __repr__(self):
        return binascii.hexlify(self.buffer)

    def __str__(self):
        return "ClientMessage:{{" \
               "length={}, " \
               "correlationId={}, " \
               "messageType={}, " \
               "partitionId={}, " \
               "isComplete={}, " \
               "isRetryable={}, " \
               "isEvent={}, " \
               "writeOffset={}}}".format(self.get_frame_length(),
                                         self.get_correlation_id(),
                                         self.get_message_type(),
                                         self.get_partition_id(),
                                         self.is_complete(),
                                         self.is_retryable(),
                                         self.is_flag_set(LISTENER_FLAG),
                                         self.get_data_offset())


class ClientMessageBuilder(object):
    def __init__(self, message_callback):
        self.logger = logging.getLogger("ClientMessageBuilder:")
        self._incomplete_messages = dict()
        self._message_callback = message_callback

    def on_message(self, client_message):
        if client_message.is_flag_set(BEGIN_END_FLAG):
            # handle message
            self._message_callback(client_message)
        elif client_message.is_flag_set(BEGIN_FLAG):
            self._incomplete_messages[client_message.get_correlation_id()] = client_message
        else:
            try:
                message = self._incomplete_messages[client_message.get_correlation_id()]
            except KeyError:
                self.logger.warning("A message without the begin part is received.")
                return
            message.accumulate(client_message)
            if client_message.is_flag_set(END_FLAG):
                message.add_flag(BEGIN_END_FLAG)
                self._message_callback(message)
                del self._incomplete_messages[client_message.get_correlation_id()]
