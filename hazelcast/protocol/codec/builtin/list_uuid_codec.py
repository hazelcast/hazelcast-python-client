from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.codec.builtin.fixed_size_types_codec import FixedSizeTypesCodec, UUID_SIZE_IN_BYTES


class ListUUIDCodec:
    @staticmethod
    def encode(client_message, collection):
        item_count = len(collection)
        frame = ClientMessage.Frame(bytearray(item_count * UUID_SIZE_IN_BYTES))
        #collection.pop(0)
        for i, value in enumerate(collection):
            FixedSizeTypesCodec.encode_uuid(frame.content, i * UUID_SIZE_IN_BYTES, value)

        client_message.add(frame)

    @staticmethod
    def decode(frame_or_iterator):
        if isinstance(frame_or_iterator, ClientMessage.ForwardFrameIterator):
            return ListUUIDCodec.decode(frame_or_iterator.next())
        else:
            item_count = len(frame_or_iterator.content) // UUID_SIZE_IN_BYTES
            result = []
            for i in range(item_count):
                result.append(FixedSizeTypesCodec.decode_uuid(frame_or_iterator.content, i * UUID_SIZE_IN_BYTES))

            return result


