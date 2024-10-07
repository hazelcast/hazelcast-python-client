from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.vector_index_config_codec import VectorIndexConfigCodec

# hex: 0x1B1400
_REQUEST_MESSAGE_TYPE = 1774592
# hex: 0x1B1401
_RESPONSE_MESSAGE_TYPE = 1774593

_REQUEST_BACKUP_COUNT_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_ASYNC_BACKUP_COUNT_OFFSET = _REQUEST_BACKUP_COUNT_OFFSET + INT_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_ASYNC_BACKUP_COUNT_OFFSET + INT_SIZE_IN_BYTES


def encode_request(name, index_configs, backup_count, async_backup_count):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_int(buf, _REQUEST_BACKUP_COUNT_OFFSET, backup_count)
    FixSizedTypesCodec.encode_int(buf, _REQUEST_ASYNC_BACKUP_COUNT_OFFSET, async_backup_count)
    StringCodec.encode(buf, name)
    ListMultiFrameCodec.encode(buf, index_configs, VectorIndexConfigCodec.encode, True)
    return OutboundMessage(buf, False)
