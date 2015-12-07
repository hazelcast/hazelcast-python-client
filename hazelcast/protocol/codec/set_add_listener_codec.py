from hazelcast.serialization.data import *
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.protocol.codec.set_message_type import *
from hazelcast.protocol.event_response_const import *

REQUEST_TYPE = SET_ADDLISTENER
RESPONSE_TYPE = 104
RETRYABLE = False


def calculate_size(name, include_value, local_only):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += BOOLEAN_SIZE_IN_BYTES
    data_size += BOOLEAN_SIZE_IN_BYTES
    return data_size


def encode_request(name, include_value, local_only):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, include_value, local_only))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_bool(include_value)
    client_message.append_bool(local_only)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message):
    """ Decode response from client message"""
    parameters = dict(response=None)
    parameters['response'] = client_message.read_str()
    return parameters


def handle(client_message, handle_event_item = None):
    """ Event handler """
    messageType = client_message.get_message_type()
    if messageType == EVENT_ITEM and handle_event_item is not None:
        if not client_message.read_bool():
            item = client_message.read_data()
        uuid = client_message.read_str()
        event_type = client_message.read_int()
        handle_event_item(client_message, item, uuid, event_type)

