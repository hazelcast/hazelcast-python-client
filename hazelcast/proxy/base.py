import logging

from hazelcast.future import make_blocking
from hazelcast.invocation import Invocation
from hazelcast.partition import string_partition_strategy
from hazelcast.util import enum, thread_id
from hazelcast import six

MAX_SIZE = float('inf')


def _no_op_response_handler(_):
    return None


class Proxy(object):
    """
    Provides basic functionality for Hazelcast Proxies.
    """
    def __init__(self, client, service_name, name):
        self.service_name = service_name
        self.name = name
        self.logger = logging.getLogger("HazelcastClient.%s(%s)" % (type(self).__name__, name))
        self._client = client
        self._invocation_service = client.invocation_service
        self._to_object = client.serialization_service.to_object
        self._to_data = client.serialization_service.to_data
        self._register_listener = client.listener_service.register_listener
        self._deregister_listener = client.listener_service.deregister_listener
        self._is_smart = client.config.network.smart_routing

    def destroy(self):
        """
        Destroys this proxy.

        :return: (bool), ``true`` if this proxy is deleted successfully, ``false`` otherwise.
        """
        self._on_destroy()
        return self._client.proxy_manager.destroy_proxy(self.service_name, self.name)

    def _on_destroy(self):
        pass

    def __repr__(self):
        return '%s(name="%s")' % (type(self).__name__, self.name)

    def _invoke(self, request, response_handler=_no_op_response_handler):
        invocation = Invocation(request, response_handler=response_handler)
        self._invocation_service.invoke(invocation)
        return invocation.future

    def _invoke_on_target(self, request, uuid, response_handler=_no_op_response_handler):
        invocation = Invocation(request, uuid=uuid, response_handler=response_handler)
        self._invocation_service.invoke(invocation)
        return invocation.future

    def _invoke_on_key(self, request, key_data, response_handler=_no_op_response_handler):
        partition_id = self._client.partition_service.get_partition_id(key_data)
        invocation = Invocation(request, partition_id=partition_id, response_handler=response_handler)
        self._invocation_service.invoke(invocation)
        return invocation.future

    def _invoke_on_partition(self, request, partition_id, response_handler=_no_op_response_handler):
        invocation = Invocation(request, partition_id=partition_id, response_handler=response_handler)
        self._invocation_service.invoke(invocation)
        return invocation.future

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
        partition_key = client.serialization_service.to_data(string_partition_strategy(self.name))
        self._partition_id = client.partition_service.get_partition_id(partition_key)

    def _invoke(self, request, response_handler=_no_op_response_handler):
        invocation = Invocation(request, partition_id=self._partition_id, response_handler=response_handler)
        self._invocation_service.invoke(invocation)
        return invocation.future


class TransactionalProxy(object):
    """
    Provides an interface for all transactional distributed objects.
    """
    def __init__(self, name, transaction):
        self.name = name
        self.transaction = transaction
        self._to_object = transaction.client.serialization_service.to_object
        self._to_data = transaction.client.serialization_service.to_data

    def _invoke(self, request, response_handler=_no_op_response_handler):
        invocation = Invocation(request, connection=self.transaction.connection, response_handler=response_handler)
        self.transaction.client.invocation_service.invoke(invocation)
        return invocation.future

    def __repr__(self):
        return '%s(name="%s")' % (type(self).__name__, self.name)


ItemEventType = enum(added=1, removed=2)
EntryEventType = enum(added=1, removed=2, updated=4, evicted=8, expired=16, evict_all=32, clear_all=64, merged=128,
                      invalidation=256, loaded=512)


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
    def __init__(self, to_object, key, value, old_value, merging_value, event_type, uuid,
                 number_of_affected_entries):
        self._to_object = to_object
        self._key_data = key
        self._value_data = value
        self._old_value_data = old_value
        self._merging_value_data = merging_value
        self.event_type = event_type
        self.uuid = uuid
        self.number_of_affected_entries = number_of_affected_entries

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
        return "EntryEvent(key=%s, value=%s, old_value=%s, merging_value=%s, event_type=%s, uuid=%s, " \
               "number_of_affected_entries=%s)" % (
                   self.key, self.value, self.old_value, self.merging_value, EntryEventType.reverse[self.event_type],
                   self.uuid, self.number_of_affected_entries)


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
    for (key, value) in six.iteritems(kwargs):
        if value:
            flags |= getattr(EntryEventType, key)
    return flags
