from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.distributed_object_info_codec import DistributedObjectInfoCodec

# hex: 0x000800
_REQUEST_MESSAGE_TYPE = 2048
# hex: 0x000801
_RESPONSE_MESSAGE_TYPE = 2049

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request():
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    msg.next_frame()
    return ListMultiFrameCodec.decode(msg, DistributedObjectInfoCodec.decode)
