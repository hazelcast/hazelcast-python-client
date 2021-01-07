import errno
import socket

from hazelcast.serialization.bits import *

SIZE_OF_FRAME_LENGTH_AND_FLAGS = INT_SIZE_IN_BYTES + SHORT_SIZE_IN_BYTES

_MESSAGE_TYPE_OFFSET = 0
_CORRELATION_ID_OFFSET = _MESSAGE_TYPE_OFFSET + INT_SIZE_IN_BYTES
_RESPONSE_BACKUP_ACKS_OFFSET = _CORRELATION_ID_OFFSET + LONG_SIZE_IN_BYTES
_PARTITION_ID_OFFSET = _CORRELATION_ID_OFFSET + LONG_SIZE_IN_BYTES
_FRAGMENTATION_ID_OFFSET = 0

_OUTBOUND_MESSAGE_MESSAGE_TYPE_OFFSET = _MESSAGE_TYPE_OFFSET + SIZE_OF_FRAME_LENGTH_AND_FLAGS
_OUTBOUND_MESSAGE_CORRELATION_ID_OFFSET = _CORRELATION_ID_OFFSET + SIZE_OF_FRAME_LENGTH_AND_FLAGS
_OUTBOUND_MESSAGE_PARTITION_ID_OFFSET = _PARTITION_ID_OFFSET + SIZE_OF_FRAME_LENGTH_AND_FLAGS

REQUEST_HEADER_SIZE = _OUTBOUND_MESSAGE_PARTITION_ID_OFFSET + INT_SIZE_IN_BYTES
RESPONSE_HEADER_SIZE = _RESPONSE_BACKUP_ACKS_OFFSET + BYTE_SIZE_IN_BYTES
EVENT_HEADER_SIZE = _PARTITION_ID_OFFSET + INT_SIZE_IN_BYTES

_DEFAULT_FLAGS = 0
_BEGIN_FRAGMENT_FLAG = 1 << 15
_END_FRAGMENT_FLAG = 1 << 14
_UNFRAGMENTED_MESSAGE_FLAGS = _BEGIN_FRAGMENT_FLAG | _END_FRAGMENT_FLAG
_IS_FINAL_FLAG = 1 << 13
_BEGIN_DATA_STRUCTURE_FLAG = 1 << 12
_END_DATA_STRUCTURE_FLAG = 1 << 11
_IS_NULL_FLAG = 1 << 10
_IS_EVENT_FLAG = 1 << 9
_IS_BACKUP_AWARE_FLAG = 1 << 8
_IS_BACKUP_EVENT_FLAG = 1 << 7


# For codecs
def create_initial_buffer(size, message_type, is_final=False):
    size += SIZE_OF_FRAME_LENGTH_AND_FLAGS
    buf = bytearray(size)
    LE_INT.pack_into(buf, 0, size)
    flags = _UNFRAGMENTED_MESSAGE_FLAGS
    if is_final:
        flags |= _IS_FINAL_FLAG
    LE_UINT16.pack_into(buf, INT_SIZE_IN_BYTES, flags)
    LE_INT.pack_into(buf, _OUTBOUND_MESSAGE_MESSAGE_TYPE_OFFSET, message_type)
    LE_INT.pack_into(buf, _OUTBOUND_MESSAGE_PARTITION_ID_OFFSET, -1)
    return buf


# For custom codecs
def create_initial_buffer_custom(size, is_begin_frame=False):
    size += SIZE_OF_FRAME_LENGTH_AND_FLAGS
    if is_begin_frame:
        # Needed for custom codecs that does not have initial frame at first
        # but requires later due to new fix sized parameters
        buf = bytearray(size)
        LE_INT.pack_into(buf, 0, size)
        LE_UINT16.pack_into(buf, INT_SIZE_IN_BYTES, _BEGIN_DATA_STRUCTURE_FLAG)
        return buf
    else:
        # also add BEGIN_FRAME_BUF
        buf = bytearray(SIZE_OF_FRAME_LENGTH_AND_FLAGS + size)
        buf[:SIZE_OF_FRAME_LENGTH_AND_FLAGS] = BEGIN_FRAME_BUF
        LE_INT.pack_into(buf, SIZE_OF_FRAME_LENGTH_AND_FLAGS, size)
        # no need to encode flags since buf is initialized with zeros
        return buf


class OutboundMessage(object):
    __slots__ = ("buf", "retryable")

    def __init__(self, buf, retryable):
        self.buf = buf
        self.retryable = retryable

    def set_correlation_id(self, correlation_id):
        LE_LONG.pack_into(self.buf, _OUTBOUND_MESSAGE_CORRELATION_ID_OFFSET, correlation_id)

    def get_correlation_id(self):
        return LE_LONG.unpack_from(self.buf, _OUTBOUND_MESSAGE_CORRELATION_ID_OFFSET)[0]

    def set_partition_id(self, partition_id):
        LE_INT.pack_into(self.buf, _OUTBOUND_MESSAGE_PARTITION_ID_OFFSET, partition_id)

    def copy(self):
        return OutboundMessage(bytearray(self.buf), self.retryable)

    def set_backup_aware_flag(self):
        flags = LE_UINT16.unpack_from(self.buf, INT_SIZE_IN_BYTES)[0]
        flags |= _IS_BACKUP_AWARE_FLAG
        LE_UINT16.pack_into(self.buf, INT_SIZE_IN_BYTES, flags)

    def __repr__(self):
        message_type = LE_INT.unpack_from(self.buf, _OUTBOUND_MESSAGE_MESSAGE_TYPE_OFFSET)[0]
        correlation_id = self.get_correlation_id()
        return "OutboundMessage(message_type=%s, correlation_id=%s, retryable=%s)" % (
            message_type,
            correlation_id,
            self.retryable,
        )


class Frame(object):
    __slots__ = ("buf", "flags", "next")

    def __init__(self, buf, flags):
        self.buf = buf
        self.flags = flags
        self.next = None

    def copy(self):
        frame = Frame(self.buf, self.flags)
        return frame

    def is_begin_frame(self):
        return self._is_flag_set(_BEGIN_DATA_STRUCTURE_FLAG)

    def is_end_frame(self):
        return self._is_flag_set(_END_DATA_STRUCTURE_FLAG)

    def is_null_frame(self):
        return self._is_flag_set(_IS_NULL_FLAG)

    def is_final_frame(self):
        return self._is_flag_set(_IS_FINAL_FLAG)

    def has_event_flag(self):
        return self._is_flag_set(_IS_EVENT_FLAG)

    def has_backup_event_flag(self):
        return self._is_flag_set(_IS_BACKUP_EVENT_FLAG)

    def has_unfragmented_message_flags(self):
        return self._is_flag_set(_UNFRAGMENTED_MESSAGE_FLAGS)

    def has_begin_fragment_flag(self):
        return self._is_flag_set(_BEGIN_FRAGMENT_FLAG)

    def has_end_fragment_flag(self):
        return self._is_flag_set(_END_FRAGMENT_FLAG)

    def _is_flag_set(self, flag_mask):
        i = self.flags & flag_mask
        return i == flag_mask


class InboundMessage(object):
    __slots__ = ("start_frame", "end_frame", "_next_frame")

    def __init__(self, start_frame):
        self.start_frame = start_frame
        self.end_frame = start_frame
        self._next_frame = start_frame

    def next_frame(self):
        result = self._next_frame
        if self._next_frame is not None:
            self._next_frame = self._next_frame.next
        return result

    def has_next_frame(self):
        return self._next_frame is not None

    def peek_next_frame(self):
        return self._next_frame

    def add_frame(self, frame):
        frame.next = None
        # For inbound messages, we always had the start_frame and end_frame set
        self.end_frame.next = frame
        self.end_frame = frame

    def get_message_type(self):
        return LE_INT.unpack_from(self.start_frame.buf, _MESSAGE_TYPE_OFFSET)[0]

    def get_correlation_id(self):
        return LE_LONG.unpack_from(self.start_frame.buf, _CORRELATION_ID_OFFSET)[0]

    def get_fragmentation_id(self):
        return LE_LONG.unpack_from(self.start_frame.buf, _FRAGMENTATION_ID_OFFSET)[0]

    def get_number_of_backup_acks(self):
        return LE_UINT8.unpack_from(self.start_frame.buf, _RESPONSE_BACKUP_ACKS_OFFSET)[0]

    def merge(self, fragment):
        # should be called after calling drop_fragmentation_frame() on fragment
        self.end_frame.next = fragment.start_frame
        self.end_frame = fragment.end_frame

    def drop_fragmentation_frame(self):
        self.start_frame = self.start_frame.next
        self._next_frame = self.start_frame

    def __repr__(self):
        message_type = self.get_message_type()
        correlation_id = self.get_correlation_id()
        return "InboundMessage(message_type=%s, correlation_id=%s)" % (message_type, correlation_id)


NULL_FRAME_BUF = bytearray(SIZE_OF_FRAME_LENGTH_AND_FLAGS)
LE_INT.pack_into(NULL_FRAME_BUF, 0, SIZE_OF_FRAME_LENGTH_AND_FLAGS)
LE_UINT16.pack_into(NULL_FRAME_BUF, INT_SIZE_IN_BYTES, _IS_NULL_FLAG)

# Has IS_NULL and IS_FINAL flags
NULL_FINAL_FRAME_BUF = bytearray(SIZE_OF_FRAME_LENGTH_AND_FLAGS)
LE_INT.pack_into(NULL_FINAL_FRAME_BUF, 0, SIZE_OF_FRAME_LENGTH_AND_FLAGS)
LE_UINT16.pack_into(NULL_FINAL_FRAME_BUF, INT_SIZE_IN_BYTES, _IS_NULL_FLAG | _IS_FINAL_FLAG)

BEGIN_FRAME_BUF = bytearray(SIZE_OF_FRAME_LENGTH_AND_FLAGS)
LE_INT.pack_into(BEGIN_FRAME_BUF, 0, SIZE_OF_FRAME_LENGTH_AND_FLAGS)
LE_UINT16.pack_into(BEGIN_FRAME_BUF, INT_SIZE_IN_BYTES, _BEGIN_DATA_STRUCTURE_FLAG)

END_FRAME_BUF = bytearray(SIZE_OF_FRAME_LENGTH_AND_FLAGS)
LE_INT.pack_into(END_FRAME_BUF, 0, SIZE_OF_FRAME_LENGTH_AND_FLAGS)
LE_UINT16.pack_into(END_FRAME_BUF, INT_SIZE_IN_BYTES, _END_DATA_STRUCTURE_FLAG)

# Has END_DATA_STRUCTURE and IS_FINAL flags
END_FINAL_FRAME_BUF = bytearray(SIZE_OF_FRAME_LENGTH_AND_FLAGS)
LE_INT.pack_into(END_FINAL_FRAME_BUF, 0, SIZE_OF_FRAME_LENGTH_AND_FLAGS)
LE_UINT16.pack_into(
    END_FINAL_FRAME_BUF, INT_SIZE_IN_BYTES, _END_DATA_STRUCTURE_FLAG | _IS_FINAL_FLAG
)


class ClientMessageBuilder(object):
    def __init__(self, message_callback):
        self._fragmented_messages = dict()
        self._message_callback = message_callback

    def on_message(self, client_message):
        if client_message.start_frame.has_unfragmented_message_flags():
            self._message_callback(client_message)
        else:
            fragmentation_frame = client_message.start_frame
            fragmentation_id = client_message.get_fragmentation_id()
            client_message.drop_fragmentation_frame()
            if fragmentation_frame.has_begin_fragment_flag():
                self._fragmented_messages[fragmentation_id] = client_message
            else:
                existing_message = self._fragmented_messages.get(fragmentation_id, None)
                if not existing_message:
                    raise socket.error(errno.EIO, "A message without the begin part is received.")

                existing_message.merge(client_message)
                if fragmentation_frame.has_end_fragment_flag():
                    self._message_callback(existing_message)
                    del self._fragmented_messages[fragmentation_id]
