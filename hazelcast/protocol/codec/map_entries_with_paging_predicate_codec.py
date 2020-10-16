from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.codec.custom.paging_predicate_holder_codec import PagingPredicateHolderCodec
from hazelcast.protocol.builtin import EntryListCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.codec.custom.anchor_data_list_holder_codec import AnchorDataListHolderCodec

# hex: 0x013600
_REQUEST_MESSAGE_TYPE = 79360
# hex: 0x013601
_RESPONSE_MESSAGE_TYPE = 79361

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, predicate):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    PagingPredicateHolderCodec.encode(buf, predicate, True)
    return OutboundMessage(buf, True)


def decode_response(msg):
    msg.next_frame()
    response = dict()
    response["response"] = EntryListCodec.decode(msg, DataCodec.decode, DataCodec.decode)
    response["anchor_data_list"] = AnchorDataListHolderCodec.decode(msg)
    return response
