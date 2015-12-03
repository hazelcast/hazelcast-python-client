import struct

# constants
VERSION = 0
BEGIN_FLAG = 0x80
END_FLAG = 0x40
BEGIN_END_FLAG = BEGIN_FLAG | END_FLAG
LISTENER_FLAG = 0x01

PAYLOAD_OFFSET = 18
SIZE_OFFSET = 0
VERSION_OFFSET = 4
FLAG_OFFSET = 5
MESSAGE_TYPE_OFFSET = FLAG_OFFSET + 1
CORRELATION_ID_OFFSET = MESSAGE_TYPE_OFFSET + 2
PARTITION_ID_OFFSET = CORRELATION_ID_OFFSET + 4
DATA_OFFSET_FIELD_OFFSET = PARTITION_ID_OFFSET + 4

class ClientMessageBuilder(object):
    """
    -The first four bytes are the total size of the message
    -The next byte is the version
    -The next byte is the flag
    -The next two bytes are the message type
    -The next four bytes are the Correlation ID - unique to each message type
    -The next four bytes are the partition ID - -1 will put the ID into the default thread pool
    -The next two bytes define the offset from the start of the frame to the payload (Currently it is only 18 because
    the message header extension isn't used yet)
    -The rest of the message is the payload.
    """
    def __init__(self):
        self._payload = []
        self._correlation_id = 0
        self._partition_id = -1
        self._message_type = 0
        self._flags = BEGIN_END_FLAG
        self._format_str = "<IBBHIiH"

    def set_correlation_id(self, val):
        self._correlation_id = val
        return self

    def get_correlation_id(self):
        return self._correlation_id;

    def set_payload(self, val):
        self._payload = val
        return self

    def set_flags(self, val):
        self._flags = val
        return self

    def set_partition_id(self, val):
        self._partition_id = val
        return self

    def set_message_type(self, val):
        self._message_type = val
        return self

    def set_str(self, val):
        self._format_str += "I" + str(len(val)) + "s"
        self._payload.append(len(val))
        self._payload.append(val)
        return self

    def set_bool(self, val):
        return self.set_byte(val)

    def set_byte(self, val):
        self._format_str += "B"
        self._payload.append(val)
        return self

    def to_bytes(self):
        total_len = struct.calcsize(self._format_str)
        args = [total_len, VERSION, self._flags, self._message_type,
                self._correlation_id,
                self._partition_id, PAYLOAD_OFFSET] + self._payload
        return struct.pack(self._format_str,  *args)

class ClientMessageParser(object):
    def __init__(self, buf):
        self._buffer = buf
        self._offset = self.get_data_offset()
        # import binascii
        # print("ClientMessageParser", binascii.hexlify(self._buffer))
        # print("get_correlation_id", self.get_correlation_id())
        # print("get_message_type", self.get_message_type())
        # print("get_flags", self.get_flags())
        # print("offset", self.get_data_offset())

    def get_data_offset(self):
        return struct.unpack_from("<H", self._buffer, DATA_OFFSET_FIELD_OFFSET)[0]

    def get_correlation_id(self):
        return struct.unpack_from("<I", self._buffer, CORRELATION_ID_OFFSET)[0]

    def get_message_type(self):
        return struct.unpack_from("<H", self._buffer, MESSAGE_TYPE_OFFSET)[0]

    def get_flags(self):
        return struct.unpack_from("<B", self._buffer, FLAG_OFFSET)[0]

    def is_listener_message(self):
        return self.get_flags() & LISTENER_FLAG

    def read_str(self):
        size = self._read_fmt("<I")
        return self._read_fmt(str(size) + "s")

    def read_bool(self):
        return self._read_fmt("?")

    def read_byte(self):
        return self._read_fmt("B")

    def read_int(self):
        return self._read_fmt("<I")

    def _read_fmt(self, fmt):
        val = struct.unpack_from(fmt, self._buffer, self._offset)
        self._offset += struct.calcsize(fmt)
        return val[0]