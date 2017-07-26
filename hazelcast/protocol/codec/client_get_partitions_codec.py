from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.util import ImmutableLazyDataList
from hazelcast.protocol.codec.client_message_type import *

REQUEST_TYPE = CLIENT_GETPARTITIONS
RESPONSE_TYPE = 108
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
    parameters = dict(partitions=None)
    partitions_size = client_message.read_int()
    partitions = []
    for partitions_index in xrange(0, partitions_size):
        partitions_item = {}
        partitions_item_key = AddressCodec.decode(client_message, to_object)
        partitions_item_val_size = client_message.read_int()
        partitions_item_val = []
        for partitions_item_val_index in xrange(0, partitions_item_val_size):
            partitions_item_val_item = client_message.read_int()
            partitions_item_val.append(partitions_item_val_item)
            partitions_item[partitions_item_key] = partitions_item_val
        parameters['partitions_item'] = partitions_item
        partitions.append(partitions_item)
    parameters['partitions'] = ImmutableLazyDataList(partitions, to_object)
    return parameters



