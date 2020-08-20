from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x080500
_REQUEST_MESSAGE_TYPE = 525568
# hex: 0x080501
_RESPONSE_MESSAGE_TYPE = 525569

_REQUEST_UUID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_UUID_OFFSET + UUID_SIZE_IN_BYTES


def encode_request(name, uuid, callable):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_UUID_OFFSET, uuid)
    StringCodec.encode(buf, name)
    DataCodec.encode(buf, callable, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    msg.next_frame()
    return CodecUtil.decode_nullable(msg, DataCodec.decode)
