from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.codec.builtin.fixed_size_types_codec import FixedSizeTypesCodec, INT_SIZE_IN_BYTES


class ListIntegerCodec:
    @staticmethod
    def encode(client_message, collection):
        item_count = len(collection)
        frame = ClientMessage.Frame(bytearray(item_count * INT_SIZE_IN_BYTES))

        for i, value in enumerate(collection):
            FixedSizeTypesCodec.encode_int(frame.content, i * INT_SIZE_IN_BYTES, value)

        client_message.add(frame)

    @staticmethod
    def decode(frame):
        if isinstance(frame, ClientMessage.ForwardFrameIterator):
            return ListIntegerCodec.decode(frame.next())
        item_count = 0 if frame.content is None else len(frame.content) // INT_SIZE_IN_BYTES
        result = []
        for i in range(item_count):
            result.append(FixedSizeTypesCodec.decode_int(frame.content, i * INT_SIZE_IN_BYTES))

        return result
