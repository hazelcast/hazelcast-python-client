from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.core import MemberInfo
from hazelcast.protocol.codec.custom.address_codec import AddressCodec
from hazelcast.protocol.builtin import MapCodec
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.codec.custom.member_version_codec import MemberVersionCodec

_UUID_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_LITE_MEMBER_OFFSET = _UUID_OFFSET + UUID_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _LITE_MEMBER_OFFSET + BOOLEAN_SIZE_IN_BYTES - 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS


class MemberInfoCodec(object):
    @staticmethod
    def encode(buf, member_info):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE, False)
        FixSizedTypesCodec.encode_uuid(initial_frame_buf, _UUID_OFFSET, member_info.uuid)
        FixSizedTypesCodec.encode_boolean(initial_frame_buf, _LITE_MEMBER_OFFSET, member_info.lite_member)
        buf.extend(initial_frame_buf)
        AddressCodec.encode(buf, member_info.address)
        MapCodec.encode(buf, member_info.attributes, StringCodec.encode, StringCodec.encode)
        MemberVersionCodec.encode(buf, member_info.version)
        MapCodec.encode(buf, member_info.address_map, EndpointQualifierCodec.encode, AddressCodec.encode)
        buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        uuid = FixSizedTypesCodec.decode_uuid(initial_frame.buf, _UUID_OFFSET)
        lite_member = FixSizedTypesCodec.decode_boolean(initial_frame.buf, _LITE_MEMBER_OFFSET)
        address = AddressCodec.decode(msg)
        attributes = MapCodec.decode(msg, StringCodec.decode, StringCodec.decode)
        version = MemberVersionCodec.decode(msg)
        is_address_map_exists = False
        address_map = None
        if not msg.peek_next_frame().is_end_frame():
            address_map = MapCodec.decode(msg, EndpointQualifierCodec.decode, AddressCodec.decode)
            is_address_map_exists = True
        CodecUtil.fast_forward_to_end_frame(msg)
        return MemberInfo(address, uuid, attributes, lite_member, version, is_address_map_exists, address_map)
