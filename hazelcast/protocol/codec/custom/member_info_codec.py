from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.core import MemberInfo

# Generated("22a7aa6d0c8c8d586989618fbecca8cf")

UUID_FIELD_OFFSET = 0
LITE_MEMBER_FIELD_OFFSET = UUID_FIELD_OFFSET + Bits.UUID_SIZE_IN_BYTES
INITIAL_FRAME_SIZE = LITE_MEMBER_FIELD_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES


class MemberInfoCodec(object):
    @staticmethod
    def encode(client_message, member_info):
        client_message.add(BEGIN_FRAME)

        initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
        FixedSizeTypesCodec.encode_uuid(initial_frame.content, UUID_FIELD_OFFSET, member_info.uuid)
        FixedSizeTypesCodec.encode_boolean(initial_frame.content, LITE_MEMBER_FIELD_OFFSET, member_info.is_lite_member())
        client_message.add(initial_frame)

        AddressCodec.encode(client_message, member_info.address)
        MapCodec.encode(client_message, member_info.attributes(), StringCodec.encode, StringCodec.encode)
        MemberVersionCodec.encode(client_message, member_info.version)

        client_message.add(END_FRAME)

    @staticmethod
    def decode(iterator):
        # begin frame
        iterator.next()

        initial_frame = iterator.next()
        uuid = FixedSizeTypesCodec.decode_uuid(initial_frame.content, UUID_FIELD_OFFSET)
        lite_member = FixedSizeTypesCodec.decode_boolean(initial_frame.content, LITE_MEMBER_FIELD_OFFSET)

        address = AddressCodec.decode(iterator)
        attributes = MapCodec.decode(iterator, StringCodec.decode, StringCodec.decode)
        version = MemberVersionCodec.decode(iterator)

        CodecUtil.fast_forward_to_end_frame(iterator)

        return MemberInfo(address, uuid, attributes, lite_member, version)