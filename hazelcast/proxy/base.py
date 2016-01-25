import logging
from hazelcast.future import make_blocking
from hazelcast.partition import string_partition_strategy
from hazelcast.util import enum


class Proxy(object):
    def __init__(self, client, service_name, name):
        self.service_name = service_name
        self.name = name
        self._client = client
        self.logger = logging.getLogger("%s(%s)" % (type(self).__name__, name))

    def destroy(self):
        return self._client.proxy.destroy_proxy(self.service_name, self.name)

    def __str__(self):
        return '%s(name="%s")' % (type(self), self.name)

    def _to_data(self, val):
        return self._client.serializer.to_data(val)

    def _to_object(self, data):
        return self._client.serializer.to_object(data)

    def _start_listening(self, request, event_handler, response_decoder, key=None):
        return self._client.listener.start_listening(request, event_handler, response_decoder, key)

    def _stop_listening(self, registration_id, request_encoder):
        return self._client.listener.stop_listening(registration_id, request_encoder)

    def _encode_invoke(self, codec, **kwargs):
        request = codec.encode_request(**kwargs)
        return self._client.invoker.invoke_on_random_target(request).continue_with(self._handle_response, codec)

    def _encode_invoke_on_key(self, codec, key_data, **kwargs):
        partition_id = self._client.partition_service.get_partition_id(key_data)
        return self._encode_invoke_on_partition(codec, partition_id, **kwargs)

    def _encode_invoke_on_partition(self, codec, partition_id, **kwargs):
        request = codec.encode_request(**kwargs)
        return self._client.invoker.invoke_on_partition(request, partition_id).continue_with(self._handle_response,
                                                                                             codec)

    def _handle_response(self, future, codec):
        response = future.result()
        if response:
            try:
                codec.decode_response
            except AttributeError:
                return
            decoded_response = codec.decode_response(response, self._to_object)
            try:
                return decoded_response['response']
            except AttributeError:
                pass

    def _get_partition_key(self):
        return string_partition_strategy(self.name)

    def blocking(self):
        """
        :return: Return a version of this proxy with only blocking method calls
        """
        return make_blocking(self)


class PartitionSpecificProxy(Proxy):
    def __init__(self, client, service_name, name):
        super(PartitionSpecificProxy, self).__init__(client, service_name, name)
        self._partition_id = self._client.partition_service.get_partition_id(name)

    def _encode_invoke_on_partition(self, codec, **kwargs):
        return super(PartitionSpecificProxy, self)._encode_invoke_on_partition(codec, self._partition_id,
                                                                               **kwargs)


class TransactionalProxy(object):
    def __init__(self, name, transaction):
        self.name = name
        self.transaction = transaction

    def destroy(self):
        raise NotImplementedError

    def transaction_id(self):
        return self.transaction.transaction_id

    def invoke(self, request):
        return self.transaction.client.invoker.invoke_on_connection(request, self.transaction.connection)

    def _to_data(self, val):
        return self.transaction.client.serializer.to_data(val)

    def _to_object(self, data):
        return self.transaction.client.serializer.to_object(data)


ItemEventType = enum(added=1, removed=2)
EntryEventType = enum(added=1,
                      removed=1 << 1,
                      updated=1 << 2,
                      evicted=1 << 3,
                      evict_all=1 << 4,
                      clear_all=1 << 5,
                      merged=1 << 6,
                      expired=1 << 7)


class ItemEvent(object):
    def __init__(self, name, item_data, event_type, member, to_object):
        self.name = name
        self._item_data = item_data
        self.event_type = event_type
        self.member = member
        self._to_object = to_object

    @property
    def item(self):
        return self._to_object(self._item_data)


class EntryEvent(object):
    def __init__(self, to_object, key, old_value, value, merging_value, event_type, uuid,
                 number_of_affected_entries):
        self._key_data = key
        self._value_data = value
        self._old_value_data = old_value
        self._merging_value_data = merging_value
        self.event_type = event_type
        self.uuid = uuid
        self.number_of_affected_entries = number_of_affected_entries
        self._to_object = to_object

    @property
    def key(self):
        return self._to_object(self._key_data)

    @property
    def old_value(self):
        return self._to_object(self._old_value_data)

    @property
    def value(self):
        return self._to_object(self._value_data)

    @property
    def merging_value(self):
        return self._to_object(self._merging_value_data)

    def __repr__(self):
        return "EntryEvent(key=%s, old_value=%s, value=%s, merging_value=%s, event_type=%s, uuid=%s, " \
               "number_of_affected_entries=%s)" % (
                   self.key, self.old_value, self.value, self.merging_value, self.event_type, self.uuid,
                   self.number_of_affected_entries)


class TopicMessage(object):
    def __init__(self, name, message_data, publish_time, member, to_object):
        self.name = name
        self._message_data = message_data
        self.publish_time = publish_time
        self.member = member
        self._to_object = to_object

    @property
    def message(self):
        return self._to_object(self._message_data)


def get_entry_listener_flags(**kwargs):
    flags = 0
    for (key, value) in kwargs.iteritems():
        if value:
            flags |= getattr(EntryEventType, key)
    return flags



