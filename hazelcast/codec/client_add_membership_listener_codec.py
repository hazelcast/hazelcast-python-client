from hazelcast.codec.util import decode_member
from hazelcast.message import ClientMessageBuilder

MESSAGE_TYPE = 0x4
EVENT_MEMBER = 200
EVENT_MEMBERLIST = 201
EVENT_MEMBERATTRIBUTECHANGE = 202


def encode_request(local_only):
    message = ClientMessageBuilder()
    message.set_message_type(MESSAGE_TYPE)
    message.set_bool(local_only)
    return message


def decode_response(message):
    params = {}
    params["response"] = message.read_str()
    return params


def handle(message, handle_event_member=None, handle_event_memberlist=None, handle_event_memberattributechange=None):
    message_type = message.get_message_type()
    if message_type == EVENT_MEMBERLIST and handle_event_memberlist is not None:
        size = message.read_int()
        members = [decode_member(message) for _ in xrange(0, size)]
        handle_event_memberlist(members)
    elif message_type == EVENT_MEMBER and handle_event_member is not None:
        member = decode_member(message)
        event_type = message.read_int()
        handle_event_member(member, event_type)
        pass
    elif message_type == EVENT_MEMBERATTRIBUTECHANGE and handle_event_memberattributechange is not None:
        raise NotImplementedError  # TODO
