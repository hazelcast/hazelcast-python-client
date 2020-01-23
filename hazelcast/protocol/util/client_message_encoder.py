from hazelcast.protocol.client_message_writer import ClientMessageWriter
import enum


class ClientMessageEncoder:
    client_message_writer = ClientMessageWriter()

    def handler_added(self):
        pass


class HandlerStatus(enum.Enum):
    CLEAN = 1
    DIRTY = 2
