from hazelcast.protocol.client_message import BEGIN_FRAME, END_FRAME, NULL_FRAME
from hazelcast.protocol.codec.builtin.codec_util import CodecUtil

class ListMultiFrameCodec:
    @staticmethod
    def encode(client_message, collection, encode_function):
        client_message.add(BEGIN_FRAME.copy())

        for item in collection:
            encode_function(client_message, item)

        client_message.add(END_FRAME.copy())

    @staticmethod
    def encode_contains_nullable(client_message, collection, encode_function):
        client_message.add(BEGIN_FRAME.copy())
        for item in collection:
            if item is None:
                client_message.add(NULL_FRAME.copy())
            else:
                encode_function(client_message, item)

        client_message.add(END_FRAME.copy())

    @staticmethod
    def encode_nullable(client_message, collection, encode_function):
        if collection is None:
            client_message.add(NULL_FRAME.copy())
        else:
            ListMultiFrameCodec.encode(client_message, collection, encode_function)

    @staticmethod
    def decode(iterator, decode_function):
        result = []

        iterator.next()
        while not CodecUtil.next_frame_is_data_structure_end_frame(iterator):
            result.append(decode_function(iterator))

        iterator.next()
        return result

    @staticmethod
    def decode_nullable(iterator, decode_function):
        if CodecUtil.next_frame_is_null_end_frame(iterator):
            return None
        else:
            return ListMultiFrameCodec.decode(iterator, decode_function)

    @staticmethod
    def decode_contains_nullable(iterator, decode_function):
        pass
        #while not CodecUtil.next_frame_is_data_structure_end_frame(iterator):


