from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME
from hazelcast.protocol.codec.builtin.codec_util import CodecUtil
from hazelcast.serialization.data import Data

class DataCodec:
    @staticmethod
    def encode(client_message, data):
        client_message.add(ClientMessage.Frame(bytearray(data)))

    @staticmethod
    def encode_nullable(client_message, data):
        if data is None:
            client_message.add(NULL_FRAME)
        else:
            client_message.add(ClientMessage.Frame(bytearray(data)))

    @staticmethod
    def decode(iterator):
        return Data(iterator.next().content)

    @staticmethod
    def decode_nullable(iterator):
        if CodecUtil.next_frame_is_null_end_frame(iterator):
            return None
        else:
            return DataCodec.decode(iterator.next())

