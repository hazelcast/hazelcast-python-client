from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.bits import INT_SIZE_IN_BYTES
from hazelcast.protocol.codec.builtin import fixed_size_types_codec


def encode(client_message, collection):
    item_count = len(collection)
    frame = ClientMessage.Frame(bytearray(item_count * INT_SIZE_IN_BYTES))

    for i, value in enumerate(collection):
        fixed_size_types_codec.encode_int(frame.content, i * INT_SIZE_IN_BYTES, value)

    client_message.add(frame)


def decode(frame):
    if isinstance(frame, ClientMessage.ForwardFrameIterator):
        return decode(frame.next())
    item_count = 0 if frame.content is None else len(frame.content) // INT_SIZE_IN_BYTES
    result = []
    for i in range(item_count):
        result.append(fixed_size_types_codec.decode_int(frame.content, i * INT_SIZE_IN_BYTES))

    return result
