from hazelcast.future import make_blocking
from hazelcast.invocation import Invocation
from hazelcast.partition import string_partition_strategy
from hazelcast import six
from hazelcast.util import get_attr_name

MAX_SIZE = float("inf")


def _no_op_response_handler(_):
    return None


class Proxy(object):
    """Provides basic functionality for Hazelcast Proxies."""

    def __init__(self, service_name, name, context):
        self.service_name = service_name
        self.name = name
        self._context = context
        self._invocation_service = context.invocation_service
        self._partition_service = context.partition_service
        serialization_service = context.serialization_service
        self._to_object = serialization_service.to_object
        self._to_data = serialization_service.to_data
        listener_service = context.listener_service
        self._register_listener = listener_service.register_listener
        self._deregister_listener = listener_service.deregister_listener
        self._is_smart = context.config.smart_routing

    def destroy(self):
        """Destroys this proxy.

        Returns:
            bool: ``True`` if this proxy is destroyed successfully, ``False`` otherwise.
        """
        self._on_destroy()
        return self._context.proxy_manager.destroy_proxy(self.service_name, self.name)

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
        partition_id = self._partition_service.get_partition_id(key_data)
        invocation = Invocation(
            request, partition_id=partition_id, response_handler=response_handler
        )
        self._invocation_service.invoke(invocation)
        return invocation.future

    def _invoke_on_partition(self, request, partition_id, response_handler=_no_op_response_handler):
        invocation = Invocation(
            request, partition_id=partition_id, response_handler=response_handler
        )
        self._invocation_service.invoke(invocation)
        return invocation.future

    def blocking(self):
        """Returns a version of this proxy with only blocking method calls."""
        return make_blocking(self)


class PartitionSpecificProxy(Proxy):
    """Provides basic functionality for Partition Specific Proxies."""

    def __init__(self, service_name, name, context):
        super(PartitionSpecificProxy, self).__init__(service_name, name, context)
        partition_key = context.serialization_service.to_data(string_partition_strategy(self.name))
        self._partition_id = context.partition_service.get_partition_id(partition_key)

    def _invoke(self, request, response_handler=_no_op_response_handler):
        invocation = Invocation(
            request, partition_id=self._partition_id, response_handler=response_handler
        )
        self._invocation_service.invoke(invocation)
        return invocation.future


class TransactionalProxy(object):
    """Provides an interface for all transactional distributed objects."""

    def __init__(self, name, transaction, context):
        self.name = name
        self.transaction = transaction
        self._invocation_service = context.invocation_service
        serialization_service = context.serialization_service
        self._to_object = serialization_service.to_object
        self._to_data = serialization_service.to_data

    def _invoke(self, request, response_handler=_no_op_response_handler):
        invocation = Invocation(
            request, connection=self.transaction.connection, response_handler=response_handler
        )
        self._invocation_service.invoke(invocation)
        return invocation.future

    def __repr__(self):
        return '%s(name="%s")' % (type(self).__name__, self.name)


class ItemEventType(object):
    """Type of item events."""

    ADDED = 1
    """
    Fired when an item is added.
    """

    REMOVED = 2
    """
    Fired when an item is removed.
    """


class EntryEventType(object):
    """Type of entry event."""

    ADDED = 1
    """
    Fired if an entry is added.
    """

    REMOVED = 2
    """
    Fired if an entry is removed.
    """

    UPDATED = 4
    """
    Fired if an entry is updated.
    """

    EVICTED = 8
    """
    Fired if an entry is evicted.
    """

    EXPIRED = 16
    """
    Fired if an entry is expired.
    """

    EVICT_ALL = 32
    """
    Fired if all entries are evicted.
    """

    CLEAR_ALL = 64
    """
    Fired if all entries are cleared.
    """

    MERGED = 128
    """
    Fired if an entry is merged after a network partition.
    """

    INVALIDATION = 256
    """
    Fired if an entry is invalidated.
    """

    LOADED = 512
    """
    Fired if an entry is loaded.
    """


class ItemEvent(object):
    """Map Item event.

    Attributes:
        name (str): Name of the proxy that fired the event.
        event_type (ItemEventType): Type of the event.
        member (hazelcast.core.MemberInfo): Member that fired the event.
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
    """Map Entry event.

    Attributes:
        event_type (EntryEventType): Type of the event.
        uuid (uuid.UUID): UUID of the member that fired the event.
        number_of_affected_entries (int): Number of affected entries by this event.
    """

    def __init__(
        self,
        to_object,
        key,
        value,
        old_value,
        merging_value,
        event_type,
        uuid,
        number_of_affected_entries,
    ):
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
        return (
            "EntryEvent(key=%s, value=%s, old_value=%s, merging_value=%s, event_type=%s, uuid=%s, "
            "number_of_affected_entries=%s)"
            % (
                self.key,
                self.value,
                self.old_value,
                self.merging_value,
                get_attr_name(EntryEventType, self.event_type),
                self.uuid,
                self.number_of_affected_entries,
            )
        )


class TopicMessage(object):
    """Topic message.

    Attributes:
        name (str): Name of the proxy that fired the event.
        publish_time (int): UNIX time that the event is published as seconds.
        member (hazelcast.core.MemberInfo): Member that fired the event.
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
