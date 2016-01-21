from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.util import ImmutableLazyDataList
from hazelcast.protocol.codec.client_message_type import *
from hazelcast.protocol.event_response_const import *

REQUEST_TYPE = CLIENT_ADDMEMBERSHIPLISTENER
RESPONSE_TYPE = 104
RETRYABLE = False


def calculate_size(local_only):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += BOOLEAN_SIZE_IN_BYTES
    return data_size


def encode_request(local_only):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(local_only))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_bool(local_only)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(response=None)
    parameters['response'] = client_message.read_str()
    return parameters


def handle(client_message, handle_event_member = None, handle_event_memberlist = None, handle_event_memberattributechange = None, to_object=None):
    """ Event handler """
    message_type = client_message.get_message_type()
    if message_type == EVENT_MEMBER and handle_event_member is not None:
        member = MemberCodec.decode(client_message, to_object)
        event_type = client_message.read_int()
        handle_event_member(member=member, event_type=event_type)
    if message_type == EVENT_MEMBERLIST and handle_event_memberlist is not None:
        members_size = client_message.read_int()
        members = []
        for members_index in xrange(0, members_size):
            members_item = MemberCodec.decode(client_message, to_object)
            members.append(members_item)
        handle_event_memberlist(members=members)
    if message_type == EVENT_MEMBERATTRIBUTECHANGE and handle_event_memberattributechange is not None:
        uuid = client_message.read_str()
        key = client_message.read_str()
        operation_type = client_message.read_int()
        value=None
        if not client_message.read_bool():
            value = client_message.read_str()
        handle_event_memberattributechange(uuid=uuid, key=key, operation_type=operation_type, value=value)

