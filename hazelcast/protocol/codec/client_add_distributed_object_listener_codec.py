from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.codec.client_message_type import *
from hazelcast.protocol.event_response_const import *

REQUEST_TYPE = CLIENT_ADDDISTRIBUTEDOBJECTLISTENER
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


def handle(client_message, handle_event_distributed_object=None, to_object=None):
    """ Event handler """
    message_type = client_message.get_message_type()
    if message_type == EVENT_DISTRIBUTEDOBJECT and handle_event_distributed_object is not None:
        name = client_message.read_str()
        service_name = client_message.read_str()
        event_type = client_message.read_str()
        handle_event_distributed_object(name=name, service_name=service_name, event_type=event_type)
