from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec

# hex: 0x0E0F00
_REQUEST_MESSAGE_TYPE = 921344
# hex: 0x0E0F01
_RESPONSE_MESSAGE_TYPE = 921345

_REQUEST_TXN_ID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_THREAD_ID_OFFSET = _REQUEST_TXN_ID_OFFSET + UUID_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_THREAD_ID_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(name, txn_id, thread_id, predicate):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_TXN_ID_OFFSET, txn_id)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_THREAD_ID_OFFSET, thread_id)
    StringCodec.encode(buf, name)
    DataCodec.encode(buf, predicate, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    msg.next_frame()
    return ListMultiFrameCodec.decode(msg, DataCodec.decode)
