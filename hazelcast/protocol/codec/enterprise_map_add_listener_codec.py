from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.protocol.codec.enterprise_map_message_type import *
from hazelcast.protocol.event_response_const import *

REQUEST_TYPE = ENTERPRISEMAP_ADDLISTENER
RESPONSE_TYPE = 104
RETRYABLE = False


def calculate_size(listener_name, local_only):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(listener_name)
    data_size += BOOLEAN_SIZE_IN_BYTES
    return data_size


def encode_request(listener_name, local_only):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(listener_name, local_only))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(listener_name)
    client_message.append_bool(local_only)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(response=None)
    parameters['response'] = client_message.read_str()
    return parameters

def handle(client_message, handle_event_querycachesingle = None, handle_event_querycachebatch = None, to_object=None):
    """ Event handler """
    message_type = client_message.get_message_type()
    if message_type == EVENT_QUERYCACHESINGLE and handle_event_querycachesingle is not None:
        data = QueryCacheEventDataCodec.decode(client_message, to_object)
        handle_event_querycachesingle(data=data)
    if message_type == EVENT_QUERYCACHEBATCH and handle_event_querycachebatch is not None:
        events_size = client_message.read_int()
        events = []
        for events_index in xrange(0, events_size):
            events_item = QueryCacheEventDataCodec.decode(client_message, to_object)
            events.append(events_item)
        source = client_message.read_str()
        partition_id = client_message.read_int()
        handle_event_querycachebatch(events=events, source=source, partition_id=partition_id)

