from hazelcast.protocol.codec.builtin import fixed_size_types_codec
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.bits import UUID_SIZE_IN_BYTES, LONG_SIZE_IN_BYTES

ENTRY_SIZE_IN_BYTES = UUID_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES


def encode(client_message, collection):
    item_count = len(collection)
    frame = ClientMessage.Frame(bytearray(item_count * ENTRY_SIZE_IN_BYTES))

    for i, value in enumerate(collection):
        fixed_size_types_codec.encode_uuid(frame.content, i * ENTRY_SIZE_IN_BYTES,i)
        fixed_size_types_codec.encode_uuid(frame.content, i * ENTRY_SIZE_IN_BYTES + UUID_SIZE_IN_BYTES, value)

    client_message.add(frame)


def decode(iterator):
    frame = iterator.next()
    item_count = len(frame.content) / ENTRY_SIZE_IN_BYTES
    result = []
    for i, value in range(item_count):
        key = fixed_size_types_codec.decode_uuid(frame.content, i * ENTRY_SIZE_IN_BYTES)
        value = fixed_size_types_codec.decode_long(frame.content, i * ENTRY_SIZE_IN_BYTES + UUID_SIZE_IN_BYTES)
        result.append((key, value))

    return result
