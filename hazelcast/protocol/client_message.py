import struct
import hazelcast.protocol.bits as Bits
import socket
import errno

# constants
VERSION = 0
BEGIN_FLAG = 0x80
END_FLAG = 0x40
BEGIN_END_FLAG = BEGIN_FLAG | END_FLAG
LISTENER_FLAG = 0x01

PAYLOAD_OFFSET = 18
SIZE_OFFSET = 0

# Note that all frames will merged with frame length and flags
TYPE_FIELD_OFFSET = 0
CORRELATION_ID_FIELD_OFFSET = TYPE_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES

# backup acks field offset is used by response messages
RESPONSE_BACKUP_ACKS_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES

# partition id field offset used by request and event messages
PARTITION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES

# offset valid for fragmentation frames only
FRAGMENTATION_ID_OFFSET = 0

DEFAULT_FLAGS = 0
BEGIN_FRAGMENT_FLAG = 1 << 15
END_FRAGMENT_FLAG = 1 << 14
UNFRAGMENTED_MESSAGE = BEGIN_FRAGMENT_FLAG | END_FRAGMENT_FLAG
IS_FINAL_FLAG = 1 << 13
BEGIN_DATA_STRUCTURE_FLAG = 1 << 12
END_DATA_STRUCTURE_FLAG = 1 << 11
IS_NULL_FLAG = 1 << 10
IS_EVENT_FLAG = 1 << 9

BACKUP_AWARE_FLAG = 1 << 8

BACKUP_EVENT_FLAG = 1 << 7

SIZE_OF_FRAME_LENGTH_AND_FLAGS = Bits.INT_SIZE_IN_BYTES + Bits.SHORT_SIZE_IN_BYTES


class ClientMessage(object):
    def __init__(self, start_frame=None, end_frame=None):
        self.start_frame = start_frame
        if end_frame is None:
            self.end_frame = start_frame
        else:
            self.end_frame = end_frame
        self.connection = None
        self.retryable = None
        self.operation_name = ""

    # BEGIN_FRAME = ClientMessage.Frame(bytearray(), BEGIN_DATA_STRUCTURE_FLAG)
    # END_FRAME = ClientMessage.Frame(bytearray(), END_DATA_STRUCTURE_FLAG)

    # @property
    # def NULL_FRAME(self):
    #    return self.Frame(bytearray(), IS_NULL_FLAG)

    @staticmethod
    def create_for_decode(start_frame):
        return ClientMessage(start_frame=start_frame)

    @staticmethod
    def create_for_encode():
        return ClientMessage()

    def add(self, frame):
        frame.next = None
        if self.start_frame is None:
            self.start_frame = frame
            self.end_frame = frame
            return self

        self.end_frame.next = frame
        self.end_frame = frame
        return self

    def frame_iterator(self):
        return self.ForwardFrameIterator(self.start_frame)

    def get_message_type(self):
        return struct.unpack_from(Bits.FMT_LE_INT, self.start_frame.content, TYPE_FIELD_OFFSET)[0]

    def set_message_type(self, message_type):
        struct.pack_into(Bits.FMT_LE_INT, self.start_frame.content, TYPE_FIELD_OFFSET, message_type)

    def get_correlation_id(self):
        return struct.unpack_from(Bits.FMT_LE_INT, self.start_frame.content, CORRELATION_ID_FIELD_OFFSET)[0]

    def set_correlation_id(self, correlation_id):
        struct.pack_into(Bits.FMT_LE_INT, self.start_frame.content, CORRELATION_ID_FIELD_OFFSET, correlation_id)

    def get_number_of_backup_acks(self):
        return struct.unpack_from(Bits.FMT_LE_LONG, self.start_frame, RESPONSE_BACKUP_ACKS_FIELD_OFFSET)[0]

    def set_number_of_backup_acks(self, number_of_backup_acks):
        struct.pack_into(Bits.FMT_LE_LONG, self.start_frame.content, RESPONSE_BACKUP_ACKS_FIELD_OFFSET,
                         number_of_backup_acks)

    def get_partition_id(self):
        return struct.unpack_from(Bits.FMT_LE_INT, self.start_frame.content, PARTITION_ID_FIELD_OFFSET)[0]

    def set_partition_id(self, partition_id):
        struct.pack_into(Bits.FMT_LE_INT, self.start_frame.content, PARTITION_ID_FIELD_OFFSET,
                         partition_id)

    @staticmethod
    def is_flag_set(flags, flag_mask):
        i = flags & flag_mask
        return i == flag_mask

    @property
    def header_flags(self):
        return self.start_frame.flags

    def get_frame_length(self):
        frame_length = 0
        current_frame = self.start_frame
        while current_frame is not None:
            frame_length += current_frame.get_size()
            current_frame = current_frame.next
        return frame_length

    def merge(self, fragment):
        # skip the first element
        fragment_message_start_frame = fragment.start_frame.next
        self.end_frame.next = fragment_message_start_frame
        self.end_frame = fragment.end_frame

    def clone(self):
        client_message = ClientMessage(start_frame=bytearray(self.start_frame.content))
        client_message.retryable=self.retryable
        return client_message

    def __eq__(self, other):
        if other is None or type(other) != type(self):
            return False
        if other is self:
            return True

        if self.retryable != other.retryable:
            return False
        if not self.operation_name == other.operation_name:
            return False
        return self.connection == other.connection

    def __str__(self):
        s = "ClientMessage[connection={0}"
        if self.start_frame is not None:
            s += ", length={}, operation={}, isRetyrable={}". \
                format(self.get_frame_length(), self.operation_name, self.retryable)

            begin_fragment = self.is_flag_set(self.start_frame.flags, BEGIN_FRAGMENT_FLAG)
            un_fragmented = self.is_flag_set(self.start_frame.flags, UNFRAGMENTED_MESSAGE)

            if un_fragmented:
                is_event = self.is_flag_set(self.start_frame.flags, IS_EVENT_FLAG)
                s += ", correlationId={}, messageType={}, isEvent={}". \
                    format(self.get_correlation_id(), self.get_message_type(), is_event)
            elif begin_fragment:
                fragmentation_id = struct.unpack_from(Bits.FMT_BE_LONG, self.start_frame.content,
                                                      FRAGMENTATION_ID_OFFSET)
                message_type = self.get_message_type()
                is_event = self.is_flag_set(self.start_frame.flags, IS_EVENT_FLAG)
                s += ", fragmentationId={}, correlationId={}, messageType={}, isEvent={}". \
                    format(fragmentation_id, self.get_correlation_id(), message_type, is_event)
            else:
                fragmentation_id = struct.unpack_from(Bits.FMT_BE_LONG, self.start_frame.content,
                                                      FRAGMENTATION_ID_OFFSET)
                s += ", fragmentationId={}".format(fragmentation_id)
            s += ", isFragmented={1}]"
            return s.format(self.connection, not un_fragmented)

    class ForwardFrameIterator(object):
        def __init__(self, start):
            self.next_frame = start

        def next(self):
            result = self.next_frame
            if self.next_frame is not None:
                self.next_frame = self.next_frame.next
            return result

        def has_next(self):
            return self.next_frame is not None

        def peek_next(self):
            return self.next_frame

    class Frame:
        def __init__(self, content, flags=None, next_frame=None):
            if flags is None:
                self.flags = DEFAULT_FLAGS
            else:
                self.flags = flags
            self.next = next_frame
            self.content = content

        def copy(self):
            frame = ClientMessage.Frame(self.content, self.flags)
            frame.next = self.next
            return frame

        def deep_copy(self):
            new_content = bytearray(self.content)
            # new_content[:] = self.content
            frame = ClientMessage.Frame(new_content, self.flags)
            frame.next = self.next
            return frame

        def is_end_frame(self):
            return ClientMessage.is_flag_set(self.flags, END_DATA_STRUCTURE_FLAG)

        def is_begin_frame(self):
            return ClientMessage.is_flag_set(self.flags, BEGIN_DATA_STRUCTURE_FLAG)

        def is_null_frame(self):
            return ClientMessage.is_flag_set(self.flags, IS_NULL_FLAG)

        def get_size(self):
            if self.content is None:
                return SIZE_OF_FRAME_LENGTH_AND_FLAGS
            else:
                return SIZE_OF_FRAME_LENGTH_AND_FLAGS + len(self.content)

        def __eq__(self, other):
            if other is None or type(self) != type(other):
                return False
            if self is other:
                return True

            if self.flags != other.flags:
                return False

            return self.content == other.content


NULL_FRAME = ClientMessage.Frame(bytearray(), IS_NULL_FLAG)
BEGIN_FRAME = ClientMessage.Frame(bytearray(), BEGIN_DATA_STRUCTURE_FLAG)
END_FRAME = ClientMessage.Frame(bytearray(), END_DATA_STRUCTURE_FLAG)

if __name__ == "__main__":
    f = ClientMessage.Frame(bytearray("dfgsd", "utf-8"))
    v = ClientMessage.Frame(bytearray())
    c = ClientMessage()
    print(type(v))
    #print(hash(c))
    print(f == v)
    print(hash(f))

class ClientMessageBuilder(object):
    def __init__(self, message_callback):
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
                raise socket.error(errno.EIO, "A message without the begin part is received.")
            message.accumulate(client_message)
            if client_message.is_flag_set(END_FLAG):
                message.add_flag(BEGIN_END_FLAG)
                self._message_callback(message)
                del self._incomplete_messages[client_message.get_correlation_id()]