from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.util import ImmutableLazyDataList
from hazelcast.protocol.codec.client_message_type import *

REQUEST_TYPE = CLIENT_REMOVEDISTRIBUTEDOBJECTLISTENER
RESPONSE_TYPE = 101
RETRYABLE = True


def calculate_size(registration_id):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(registration_id)
    return data_size


def encode_request(registration_id):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(registration_id))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(registration_id)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(response=None)
    parameters['response'] = client_message.read_bool()
    return parameters



