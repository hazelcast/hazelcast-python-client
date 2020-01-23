from hazelcast.protocol.client_message import ClientMessage, BEGIN_FRAGMENT_FLAG, END_FRAGMENT_FLAG, FRAGMENTATION_ID_OFFSET, UNFRAGMENTED_MESSAGE
from hazelcast.config import ClientProperties
from hazelcast.protocol.client_message_reader import ClientMessageReader
import struct, enum
from hazelcast.protocol.bits import FMT_LE_LONG


class ClientMessageDecoder:
    def __init__(self, connection):

        self.connection = connection

        self.active_reader = ClientMessageReader(self.connection)
        self.builder_by_session_id_map = {}


    def handler_added(self):
        bytearray()

    def on_read(self):
        while self.active_reader.connection.bytes_written - self.active_reader.connection.bytes_read:
            complete = self.active_reader.read_from(self.active_reader.connection._read_buffer)
            if not complete:
                break

            first_frame = self.active_reader.client_message.start_frame
            flags = first_frame.flags
            if ClientMessage.is_flag_set(flags, UNFRAGMENTED_MESSAGE):
                self.handle_message(self.active_reader.client_message)
            else:
                frame_iterator = self.active_reader.client_message.frame_iterator()
                # ignore the fragmentation frame
                frame_iterator.next()
                start_frame = frame_iterator.next()
                fragmentation_id = struct.unpack_from(FMT_LE_LONG, first_frame.content, FRAGMENTATION_ID_OFFSET)
                if ClientMessage.is_flag_set(flags, BEGIN_FRAGMENT_FLAG):
                    self.builder_by_session_id_map[fragmentation_id] = ClientMessage.create_for_decode(start_frame)
                elif ClientMessage.is_flag_set(flags, END_FRAGMENT_FLAG):
                    client_message = self.merge_into_existing_client_message(fragmentation_id)
                    self.handle_message(client_message)
                else:
                    self.merge_into_existing_client_message(fragmentation_id)

            self.active_reader = ClientMessageReader(self.connection)
        return HandlerStatus.CLEAN

    def merge_into_existing_client_message(self, fragmentation_id):
        existing_message = self.builder_by_session_id_map[fragmentation_id]
        existing_message.merge(self.active_reader.client_message)
        return existing_message

    def handle_message(self, client_message):
        client_message.connection = self.connection
        self.connection.callback(client_message)


class HandlerStatus(enum.Enum):
    CLEAN = 1
