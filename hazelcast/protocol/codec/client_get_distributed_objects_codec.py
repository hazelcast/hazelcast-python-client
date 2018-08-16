from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.util import ImmutableLazyDataList
from hazelcast.protocol.codec.client_message_type import *
from hazelcast.six.moves import range

REQUEST_TYPE = CLIENT_GETDISTRIBUTEDOBJECTS
RESPONSE_TYPE = 110
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


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(response=None)
    response_size = client_message.read_int()
    response = []
    for _ in range(0, response_size):
        response_item = DistributedObjectInfoCodec.decode(client_message)
        response.append(response_item)
    parameters['response'] = ImmutableLazyDataList(response, to_object)
    return parameters
