from hazelcast.protocol.codec.builtin.fixed_size_types_codec import FixedSizeTypesCodec, LONG_SIZE_IN_BYTES
from hazelcast.protocol.client_message import ClientMessage


class LongArrayCodec:
    @staticmethod
    def encode(client_message, array):
        item_count = len(array)
        frame = ClientMessage.Frame(bytearray(item_count * LONG_SIZE_IN_BYTES))
        for i in range(item_count):
            FixedSizeTypesCodec.encode_long(frame.content, i * LONG_SIZE_IN_BYTES, array[i])

        client_message.add(frame)

    @staticmethod
    def decode(frame_or_iterator):
        if type(frame_or_iterator) == type(ClientMessage.ForwardFrameIterator()):
            return LongArrayCodec.decode(frame_or_iterator.next())
        else:
            item_count = len(frame_or_iterator.content) / LONG_SIZE_IN_BYTES
            result = []
            for i in range(item_count):
                result.append(FixedSizeTypesCodec.decode_long(frame_or_iterator.content, i * LONG_SIZE_IN_BYTES))

            return result
