from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.bits import UUID_SIZE_IN_BYTES
from hazelcast.protocol.codec.builtin import fixed_size_types_codec


def encode(client_message, collection):
    item_count = len(collection)
    frame = ClientMessage.Frame(bytearray(item_count * UUID_SIZE_IN_BYTES))
    for i, value in enumerate(collection):
        fixed_size_types_codec.encode_uuid(frame.content, i * UUID_SIZE_IN_BYTES, value)

    client_message.add(frame)


def decode(frame_or_iterator):
    if isinstance(frame_or_iterator, ClientMessage.ForwardFrameIterator):
        return decode(frame_or_iterator.next())
    else:
        item_count = len(frame_or_iterator.content) // UUID_SIZE_IN_BYTES
        result = []
        for i in range(item_count):
            result.append(fixed_size_types_codec.decode_uuid(frame_or_iterator.content, i * UUID_SIZE_IN_BYTES))

        return result
