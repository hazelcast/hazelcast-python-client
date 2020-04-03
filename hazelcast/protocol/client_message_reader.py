from hazelcast.protocol.client_message import ClientMessage, IS_FINAL_FLAG, SIZE_OF_FRAME_LENGTH_AND_FLAGS
from hazelcast.protocol.bits import FMT_LE_INT, FMT_LE_UINT16, INT_SIZE_IN_BYTES, SHORT_SIZE_IN_BYTES
import struct

INT_MASK = 0xffff
MAX_INT_VALUE = 0x7fffffff


class ClientMessageReader(object):
    def __init__(self, connection):
        self.read_offset = -1
        self.connection = connection
        self.client_message = None

    def read_from(self, src):
        while True:
            if self.read_frame(src):
                if ClientMessage.is_flag_set(self.client_message.end_frame.flags, IS_FINAL_FLAG):
                    return True
                self.read_offset = -1
            else:
                return False

    def reset(self):
        self.read_offset = -1
        self.client_message = None

    # src will be filled with message that received from socket
    def read_frame(self, src):
        remaining = self.connection.bytes_written - self.connection.bytes_read
        if remaining < SIZE_OF_FRAME_LENGTH_AND_FLAGS:
            return False
        if self.read_offset == -1:
            frame_length = struct.unpack_from(FMT_LE_INT, src, self.connection.bytes_read)[0]
            if frame_length > remaining:
                return False
            self.read_offset += INT_SIZE_IN_BYTES
            assert not frame_length < SIZE_OF_FRAME_LENGTH_AND_FLAGS

            flags = struct.unpack_from(FMT_LE_UINT16, src, self.connection.bytes_read+INT_SIZE_IN_BYTES)[0] & INT_MASK
            self.read_offset += SHORT_SIZE_IN_BYTES
            self.connection.bytes_read += SIZE_OF_FRAME_LENGTH_AND_FLAGS

            size = frame_length - SIZE_OF_FRAME_LENGTH_AND_FLAGS
            byte_array = bytearray(size)

            frame = ClientMessage.Frame(byte_array, flags)
            if self.client_message is None:
                self.client_message = ClientMessage.create_for_decode(frame)
            else:
                self.client_message.add(frame)
            self.read_offset = 0
            if size == 0:
                return True
#       print(self.client_message)
        frame = self.client_message.end_frame

        return self.accumulate(src, frame.content, len(frame.content) - self.read_offset)

    def accumulate(self, src, dest, length):
        remaining = self.connection.bytes_written - self.connection.bytes_read
        read_length = min(remaining, length)
#       remaining if remaining < length else length
        if read_length > 0:
            dest[self.read_offset: self.read_offset + read_length] =\
                src[self.connection.bytes_read:self.connection.bytes_read + read_length]

            self.connection.bytes_read += read_length
            self.read_offset += read_length
            return read_length == self.read_offset
        return False
