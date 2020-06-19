from hazelcast.protocol.client_message import ClientMessage


def encode(client_message, byte_array):
    client_message.add(ClientMessage.Frame(byte_array))


def decode(frame_or_iterator):
    if isinstance(frame_or_iterator, ClientMessage.ForwardFrameIterator):
        return decode(frame_or_iterator.next())
    else:
        return frame_or_iterator.content
