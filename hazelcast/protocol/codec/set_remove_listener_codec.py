from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.codec.set_message_type import *

REQUEST_TYPE = SET_REMOVELISTENER
RESPONSE_TYPE = 101
RETRYABLE = True


def calculate_size(name, registration_id):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += calculate_size_str(registration_id)
    return data_size


def encode_request(name, registration_id):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, registration_id))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_str(registration_id)
    client_message.update_frame_length()
    return client_message

# Empty decode_response because response is not used to determine the return value.

