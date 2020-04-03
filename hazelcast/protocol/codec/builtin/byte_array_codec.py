from hazelcast.protocol.client_message import ClientMessage


class ByteArrayCodec:
    @staticmethod
    def encode(client_message, byte_array):
        client_message.add(ClientMessage.Frame(byte_array))

    @staticmethod
    def decode(frame_or_iterator):
        if type(frame_or_iterator) == type(ClientMessage.ForwardFrameIterator(bytearray())):
            return ByteArrayCodec.decode(frame_or_iterator.next())
        else:
            return frame_or_iterator.content
