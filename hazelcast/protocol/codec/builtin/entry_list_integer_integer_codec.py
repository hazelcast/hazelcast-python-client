from hazelcast.protocol.codec.builtin.fixed_size_types_codec import FixedSizeTypesCodec, INT_SIZE_IN_BYTES
from hazelcast.protocol.client_message import ClientMessage

ENTRY_SIZE_IN_BYTES = INT_SIZE_IN_BYTES * 2


class EntryListIntegerIntegerCodec:
    @staticmethod
    def encode(client_message, collection):
        item_count = len(collection)
        frame = ClientMessage.Frame(bytearray(item_count * ENTRY_SIZE_IN_BYTES))
        for i, value in enumerate(collection):
            FixedSizeTypesCodec.encode_int(frame.content, i * ENTRY_SIZE_IN_BYTES, i)
            FixedSizeTypesCodec.encode_int(frame.content, i * ENTRY_SIZE_IN_BYTES + INT_SIZE_IN_BYTES, value)
        client_message.add(frame)

    @staticmethod
    def decode(iterator):
        frame = iterator.next()
        item_count = len(frame.content) / ENTRY_SIZE_IN_BYTES
        result = {}
        for i in range(item_count):
            key = FixedSizeTypesCodec.decode_int(frame.content, i*ENTRY_SIZE_IN_BYTES)
            value = FixedSizeTypesCodec.decode_int(frame.content, i*ENTRY_SIZE_IN_BYTES + INT_SIZE_IN_BYTES)
            result[key] = value
        return result



