import abc
import typing
import uuid

from hazelcast.core import MemberInfo
from hazelcast.invocation import Invocation
from hazelcast.partition import string_partition_strategy
from hazelcast.types import KeyType, ValueType, ItemType, MessageType, BlockingProxyType
from hazelcast.util import get_attr_name

MAX_SIZE = float("inf")


def _no_op_response_handler(_):
    return None


class Proxy(typing.Generic[BlockingProxyType], abc.ABC):
    """Provides basic functionality for Hazelcast Proxies."""

    def __init__(self, service_name: str, name: str, context):
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

    def destroy(self) -> bool:
        """Destroys this proxy.

        Returns:
            bool: ``True`` if this proxy is destroyed successfully,
            ``False`` otherwise.
        """
        self._on_destroy()
        return self._context.proxy_manager.destroy_proxy(self.service_name, self.name)

    def _on_destroy(self):
        pass

    def __repr__(self) -> str:
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

    @abc.abstractmethod
    def blocking(self) -> BlockingProxyType:
        """Returns a version of this proxy with only blocking method calls."""
        pass


class PartitionSpecificProxy(Proxy[BlockingProxyType], abc.ABC):
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


class TransactionalProxy:
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
        return invocation.future.result()

    def __repr__(self):
        return '%s(name="%s")' % (type(self).__name__, self.name)


class ItemEventType:
    """Type of item events."""

    ADDED = 1
    """
    Fired when an item is added.
    """

    REMOVED = 2
    """
    Fired when an item is removed.
    """


class EntryEventType:
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


class ItemEvent(typing.Generic[ItemType]):
    """Map Item event.

    Attributes:
        name (str): Name of the proxy that fired the event.
        event_type (ItemEventType): Type of the event.
        member (MemberInfo): Member that fired the event.
    """

    def __init__(self, name: str, item_data, event_type: int, member: MemberInfo, to_object):
        self.name = name
        self._item_data = item_data
        self.event_type = event_type
        self.member = member
        self._to_object = to_object

    @property
    def item(self) -> ItemType:
        """The item related to the event."""
        return self._to_object(self._item_data)


class EntryEvent(typing.Generic[KeyType, ValueType]):
    """Map Entry event.

    Attributes:
        event_type (EntryEventType): Type of the event.
        uuid (uuid.UUID): UUID of the member that fired the event.
        number_of_affected_entries (int): Number of affected entries by this
            event.
    """

    def __init__(
        self,
        to_object,
        key,
        value,
        old_value,
        merging_value,
        event_type: int,
        member_uuid: uuid.UUID,
        number_of_affected_entries: int,
    ):
        self._to_object = to_object
        self._key_data = key
        self._value_data = value
        self._old_value_data = old_value
        self._merging_value_data = merging_value
        self.event_type = event_type
        self.uuid = member_uuid
        self.number_of_affected_entries = number_of_affected_entries

    @property
    def key(self) -> KeyType:
        """The key of this entry event."""
        return self._to_object(self._key_data)

    @property
    def old_value(self) -> ValueType:
        """The old value of the entry event."""
        return self._to_object(self._old_value_data)

    @property
    def value(self) -> ValueType:
        """The value of the entry event."""
        return self._to_object(self._value_data)

    @property
    def merging_value(self) -> ValueType:
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


_SENTINEL = object()


class TopicMessage(typing.Generic[MessageType]):
    """Topic message."""

    __slots__ = ("_name", "_message_data", "_message", "_publish_time", "_member", "_to_object")

    def __init__(self, name, message_data, publish_time, member, to_object):
        self._name = name
        self._message_data = message_data
        self._message = _SENTINEL
        self._publish_time = publish_time
        self._member = member
        self._to_object = to_object

    @property
    def name(self) -> str:
        """str: Name of the proxy that fired the event."""
        return self._name

    @property
    def publish_time(self) -> int:
        """int: UNIX time that the event is published as seconds."""
        return self._publish_time

    @property
    def member(self) -> MemberInfo:
        """MemberInfo: Member that fired the event."""
        return self._member

    @property
    def message(self) -> MessageType:
        """The message sent to Topic."""
        if self._message is not _SENTINEL:
            return self._message

        self._message = self._to_object(self._message_data)
        return self._message

    def __repr__(self):
        return "TopicMessage(message=%s, publish_time=%s, topic_name=%s, publishing_member=%s)" % (
            self.message,
            self.publish_time,
            self.name,
            self.member,
        )


def get_entry_listener_flags(**kwargs):
    flags = 0
    for key, value in kwargs.items():
        if value:
            flags |= getattr(EntryEventType, key)
    return flags
