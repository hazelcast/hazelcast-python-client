from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.codec.executor_service_message_type import *

REQUEST_TYPE = EXECUTORSERVICE_CANCELONPARTITION
RESPONSE_TYPE = 101
RETRYABLE = False


def calculate_size(uuid, partition_id, interrupt):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(uuid)
    data_size += INT_SIZE_IN_BYTES
    data_size += BOOLEAN_SIZE_IN_BYTES
    return data_size


def encode_request(uuid, partition_id, interrupt):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(uuid, partition_id, interrupt))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(uuid)
    client_message.append_int(partition_id)
    client_message.append_bool(interrupt)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(response=None)
    parameters['response'] = client_message.read_bool()
    return parameters
