from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import CodecUtil
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.address_codec import AddressCodec

# hex: 0x000100
_REQUEST_MESSAGE_TYPE = 256
# hex: 0x000101
_RESPONSE_MESSAGE_TYPE = 257

_REQUEST_UUID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_SERIALIZATION_VERSION_OFFSET = _REQUEST_UUID_OFFSET + UUID_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_SERIALIZATION_VERSION_OFFSET + BYTE_SIZE_IN_BYTES
_RESPONSE_STATUS_OFFSET = RESPONSE_HEADER_SIZE
_RESPONSE_MEMBER_UUID_OFFSET = _RESPONSE_STATUS_OFFSET + BYTE_SIZE_IN_BYTES
_RESPONSE_SERIALIZATION_VERSION_OFFSET = _RESPONSE_MEMBER_UUID_OFFSET + UUID_SIZE_IN_BYTES
_RESPONSE_PARTITION_COUNT_OFFSET = _RESPONSE_SERIALIZATION_VERSION_OFFSET + BYTE_SIZE_IN_BYTES
_RESPONSE_CLUSTER_ID_OFFSET = _RESPONSE_PARTITION_COUNT_OFFSET + INT_SIZE_IN_BYTES
_RESPONSE_FAILOVER_SUPPORTED_OFFSET = _RESPONSE_CLUSTER_ID_OFFSET + UUID_SIZE_IN_BYTES


def encode_request(cluster_name, username, password, uuid, client_type, serialization_version, client_hazelcast_version, client_name, labels):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_UUID_OFFSET, uuid)
    FixSizedTypesCodec.encode_byte(buf, _REQUEST_SERIALIZATION_VERSION_OFFSET, serialization_version)
    StringCodec.encode(buf, cluster_name)
    CodecUtil.encode_nullable(buf, username, StringCodec.encode)
    CodecUtil.encode_nullable(buf, password, StringCodec.encode)
    StringCodec.encode(buf, client_type)
    StringCodec.encode(buf, client_hazelcast_version)
    StringCodec.encode(buf, client_name)
    ListMultiFrameCodec.encode(buf, labels, StringCodec.encode, True)
    return OutboundMessage(buf, True)


def decode_response(msg):
    initial_frame = msg.next_frame()
    response = dict()
    response["status"] = FixSizedTypesCodec.decode_byte(initial_frame.buf, _RESPONSE_STATUS_OFFSET)
    response["member_uuid"] = FixSizedTypesCodec.decode_uuid(initial_frame.buf, _RESPONSE_MEMBER_UUID_OFFSET)
    response["serialization_version"] = FixSizedTypesCodec.decode_byte(initial_frame.buf, _RESPONSE_SERIALIZATION_VERSION_OFFSET)
    response["partition_count"] = FixSizedTypesCodec.decode_int(initial_frame.buf, _RESPONSE_PARTITION_COUNT_OFFSET)
    response["cluster_id"] = FixSizedTypesCodec.decode_uuid(initial_frame.buf, _RESPONSE_CLUSTER_ID_OFFSET)
    response["failover_supported"] = FixSizedTypesCodec.decode_boolean(initial_frame.buf, _RESPONSE_FAILOVER_SUPPORTED_OFFSET)
    response["address"] = CodecUtil.decode_nullable(msg, AddressCodec.decode)
    response["server_hazelcast_version"] = StringCodec.decode(msg)
    return response
