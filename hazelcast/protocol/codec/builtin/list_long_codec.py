from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.codec.builtin.fixed_size_types_codec import FixedSizeTypesCodec, LONG_SIZE_IN_BYTES

class ListLongCodec:
    @staticmethod
    def encode(client_message, collection):
        item_count = len(collection)
        frame = ClientMessage.Frame(bytearray(item_count * LONG_SIZE_IN_BYTES))

        for i, value in enumerate(collection):
            FixedSizeTypesCodec.encode_long(frame.content, i * LONG_SIZE_IN_BYTES, value)

        client_message.add(frame)

    @staticmethod
    def decode(frame):
        item_count = 0 if frame.content is None else len(frame.content) / LONG_SIZE_IN_BYTES
        result = []
        for i in range(item_count):
            result.append(FixedSizeTypesCodec.decode_long(frame.content, i * LONG_SIZE_IN_BYTES))

        return result
