from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.bits import LONG_SIZE_IN_BYTES
from hazelcast.protocol.codec.builtin import fixed_size_types_codec


def encode(client_message, array):
    item_count = len(array)
    frame = ClientMessage.Frame(bytearray(item_count * LONG_SIZE_IN_BYTES))
    for i in range(item_count):
        fixed_size_types_codec.encode_long(frame.content, i * LONG_SIZE_IN_BYTES, array[i])

    client_message.add(frame)


def decode(frame_or_iterator):
    if isinstance(frame_or_iterator, ClientMessage.ForwardFrameIterator):
        return decode(frame_or_iterator.next())
    else:
        item_count = len(frame_or_iterator.content) / LONG_SIZE_IN_BYTES
        result = []
        for i in range(item_count):
            result.append(fixed_size_types_codec.decode_long(frame_or_iterator.content, i * LONG_SIZE_IN_BYTES))

        return result
