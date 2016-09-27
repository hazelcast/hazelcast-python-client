import logging

from hazelcast.future import make_blocking
from hazelcast.partition import string_partition_strategy
from hazelcast.util import enum, thread_id


def default_response_handler(future, codec, to_object):
    response = future.result()
    if response:
        try:
            codec.decode_response
        except AttributeError:
            return
        decoded_response = codec.decode_response(response, to_object)
        try:
            return decoded_response['response']
        except AttributeError:
            pass


class Proxy(object):
    """
    Provides basic functionality for Hazelcast Proxies.
    """
    def __init__(self, client, service_name, name):
        self.service_name = service_name
        self.name = name
        self.partition_key = string_partition_strategy(self.name)
        self._client = client
        self.logger = logging.getLogger("%s(%s)" % (type(self).__name__, name))
        self._to_object = client.serialization_service.to_object
        self._to_data = client.serialization_service.to_data
        self._start_listening = client.listener.start_listening
        self._stop_listening = client.listener.stop_listening

    def destroy(self):
        """
        Destroys this proxy.

        :return: (bool), ``true`` if this proxy is deleted successfully, ``false`` otherwise.
        """
        self._on_destroy()
        return self._client.proxy.destroy_proxy(self.service_name, self.name)

    def _on_destroy(self):
        pass

    def __repr__(self):
        return '%s(name="%s")' % (type(self).__name__, self.name)

    def _encode_invoke(self, codec, response_handler=default_response_handler, **kwargs):
        request = codec.encode_request(name=self.name, **kwargs)
        return self._client.invoker.invoke_on_random_target(request).continue_with(response_handler, codec, self._to_object)

    def _encode_invoke_on_target(self, codec, _address, response_handler=default_response_handler, **kwargs):
        request = codec.encode_request(name=self.name, **kwargs)
        return self._client.invoker.invoke_on_target(request, _address).continue_with(response_handler, codec, self._to_object)

    def _encode_invoke_on_key(self, codec, key_data, **kwargs):
        partition_id = self._client.partition_service.get_partition_id(key_data)
        return self._encode_invoke_on_partition(codec, partition_id, **kwargs)

    def _encode_invoke_on_partition(self, codec, _partition_id, response_handler=default_response_handler, **kwargs):
        request = codec.encode_request(name=self.name, **kwargs)
        return self._client.invoker.invoke_on_partition(request, _partition_id).continue_with(response_handler, codec,
                                                                                              self._to_object)

    def blocking(self):
        """
        Returns a version of this proxy with only blocking method calls.
        :return: (Proxy), a version of this proxy with only blocking method calls.
        """
        return make_blocking(self)


class PartitionSpecificProxy(Proxy):
    """
    Provides basic functionality for Partition Specific Proxies.
    """
    def __init__(self, client, service_name, name):
        super(PartitionSpecificProxy, self).__init__(client, service_name, name)
        self._partition_id = self._client.partition_service.get_partition_id(self.partition_key)

    def _encode_invoke(self, codec, response_handler=default_response_handler, **kwargs):
        return super(PartitionSpecificProxy, self)._encode_invoke_on_partition(codec, self._partition_id,
                                                                               response_handler=response_handler, **kwargs)


class TransactionalProxy(object):
    """
    Provides an interface for all transactional distributed objects.
    """
    def __init__(self, name, transaction):
        self.name = name
        self.transaction = transaction
        self._to_object = transaction.client.serialization_service.to_object
        self._to_data = transaction.client.serialization_service.to_data

    def _encode_invoke(self, codec, response_handler=default_response_handler, **kwargs):
        request = codec.encode_request(name=self.name, txn_id=self.transaction.id, thread_id=thread_id(), **kwargs)
        return self.transaction.client.invoker.invoke_on_connection(request, self.transaction.connection).continue_with(
                response_handler, codec, self._to_object)

    def __repr__(self):
        return '%s(name="%s")' % (type(self).__name__, self.name)


ItemEventType = enum(added=1, removed=2)
EntryEventType = enum(added=1, removed=2, updated=4, evicted=8, evict_all=16, clear_all=32, merged=64, expired=128, invalidation=256)


class ItemEvent(object):
    """
    Map Item event.
    """
    def __init__(self, name, item_data, event_type, member, to_object):
        self.name = name
        self._item_data = item_data
        self.event_type = event_type
        self.member = member
        self._to_object = to_object

    @property
    def item(self):
        """The item related to the event."""
        return self._to_object(self._item_data)


class EntryEvent(object):
    """
    Map Entry event.
    """
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
        """The key of this entry event."""
        return self._to_object(self._key_data)

    @property
    def old_value(self):
        """The old value of the entry event."""
        return self._to_object(self._old_value_data)

    @property
    def value(self):
        """The value of the entry event."""
        return self._to_object(self._value_data)

    @property
    def merging_value(self):
        """The incoming merging value of the entry event."""
        return self._to_object(self._merging_value_data)

    def __repr__(self):
        return "EntryEvent(key=%s, old_value=%s, value=%s, merging_value=%s, event_type=%s, uuid=%s, " \
               "number_of_affected_entries=%s)" % (
                   self.key, self.old_value, self.value, self.merging_value, self.event_type, self.uuid,
                   self.number_of_affected_entries)


class TopicMessage(object):
    """
    Topic message.
    """
    def __init__(self, name, message_data, publish_time, member, to_object):
        self.name = name
        self._message_data = message_data
        self.publish_time = publish_time
        self.member = member
        self._to_object = to_object

    @property
    def message(self):
        """The message sent to Topic."""
        return self._to_object(self._message_data)


def get_entry_listener_flags(**kwargs):
    flags = 0
    for (key, value) in kwargs.iteritems():
        if value:
            flags |= getattr(EntryEventType, key)
    return flags
