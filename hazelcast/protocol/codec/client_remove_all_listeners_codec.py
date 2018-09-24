from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.codec.client_message_type import *

REQUEST_TYPE = CLIENT_REMOVEALLLISTENERS
RESPONSE_TYPE = 100
RETRYABLE = False


def calculate_size():
    """ Calculates the request payload size"""
    data_size = 0
    return data_size


def encode_request():
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size())
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.update_frame_length()
    return client_message


# Empty decode_response(client_message), this message has no parameters to decode
