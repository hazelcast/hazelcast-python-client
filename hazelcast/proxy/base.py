import abc
import typing
import uuid

from hazelcast.core import MemberInfo
from hazelcast.types import KeyType, ValueType, ItemType, MessageType, BlockingProxyType
from hazelcast.invocation import Invocation
from hazelcast.partition import string_partition_strategy
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
        self._send_schema_and_retry = context.compact_schema_service.send_schema_and_retry

    def destroy(self) -> bool:
        """Destroys this proxy.

        Returns:
            ``True`` if this proxy is destroyed successfully, ``False``
            otherwise.
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
        self._send_schema_and_retry = context.compact_schema_service.send_schema_and_retry

    def _send_schema(self, error):
        return self._send_schema_and_retry(error, lambda: None).result()

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
        name: Name of the proxy that fired the event.
        item: The item related to the event.
        event_type: Type of the event.
        member: Member that fired the event.
    """

    def __init__(self, name: str, item: ItemEventType, event_type: int, member: MemberInfo):
        self.name = name
        self.item = item
        self.event_type = event_type
        self.member = member


class EntryEvent(typing.Generic[KeyType, ValueType]):
    """Map Entry event.

    Attributes:
        event_type: Type of the event.
        uuid: UUID of the member that fired the event.
        number_of_affected_entries: Number of affected entries by this event.
        key: The key of this entry event.
        value: The value of the entry event.
        old_value: The old value of the entry event.
        merging_value: The incoming merging value of the entry event.
    """

    def __init__(
        self,
        key: KeyType,
        value: ValueType,
        old_value: ValueType,
        merging_value: ValueType,
        event_type: int,
        member_uuid: uuid.UUID,
        number_of_affected_entries: int,
    ):
        self.key = key
        self.value = value
        self.old_value = old_value
        self.merging_value = merging_value
        self.event_type = event_type
        self.uuid = member_uuid
        self.number_of_affected_entries = number_of_affected_entries

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


class TopicMessage(typing.Generic[MessageType]):
    """Topic message.

    Attributes:
        name: Name of the proxy that fired the event.
        message: The message sent to Topic.
        publish_time: UNIX time that the event is published as seconds.
        member: Member that fired the event.
    """

    __slots__ = ("name", "message", "publish_time", "member")

    def __init__(self, name: str, message: MessageType, publish_time: int, member: MemberInfo):
        self.name = name
        self.message = message
        self.publish_time = publish_time
        self.member = member

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
