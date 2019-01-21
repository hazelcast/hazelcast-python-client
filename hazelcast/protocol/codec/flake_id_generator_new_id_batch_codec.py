from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.codec.flake_id_generator_message_type import *

REQUEST_TYPE = FLAKEIDGENERATOR_NEWIDBATCH
RESPONSE_TYPE = 126
RETRYABLE = True


def calculate_size(name, batch_size):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += INT_SIZE_IN_BYTES
    return data_size


def encode_request(name, batch_size):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, batch_size))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_int(batch_size)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(base=None, increment=None, batch_size=None)
    parameters['base'] = client_message.read_long()
    parameters['increment'] = client_message.read_long()
    parameters['batch_size'] = client_message.read_int()
    return parameters



