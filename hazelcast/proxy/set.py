import typing

from hazelcast.future import Future
from hazelcast.protocol.codec import (
    set_add_all_codec,
    set_add_codec,
    set_add_listener_codec,
    set_clear_codec,
    set_compare_and_remove_all_codec,
    set_compare_and_retain_all_codec,
    set_contains_all_codec,
    set_contains_codec,
    set_get_all_codec,
    set_is_empty_codec,
    set_remove_codec,
    set_remove_listener_codec,
    set_size_codec,
)

from hazelcast.proxy.base import PartitionSpecificProxy, ItemEvent, ItemEventType
from hazelcast.types import ItemType
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import check_not_none, ImmutableLazyDataList


class Set(PartitionSpecificProxy, typing.Generic[ItemType]):
    """Concurrent, distributed implementation of Set"""

    def add(self, item: ItemType) -> Future[bool]:
        """Adds the specified item if it is not exists in this set.

        Args:
            item: The specified item to be added.

        Returns:
            ``True`` if this set is changed after call, ``False`` otherwise.
        """
        check_not_none(item, "Value can't be None")
        try:
            element_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.add, item)

        request = set_add_codec.encode_request(self.name, element_data)
        return self._invoke(request, set_add_codec.decode_response)

    def add_all(self, items: typing.Sequence[ItemType]) -> Future[bool]:
        """Adds the elements in the specified collection if they're not exist
        in this set.

        Args:
            items: Collection which includes the items to be added.

        Returns:
            ``True`` if this set is changed after call, ``False`` otherwise.
        """
        check_not_none(items, "Value can't be None")
        try:
            data_items = []
            for item in items:
                check_not_none(item, "Value can't be None")
                data_items.append(self._to_data(item))
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.add_all, items)

        request = set_add_all_codec.encode_request(self.name, data_items)
        return self._invoke(request, set_add_all_codec.decode_response)

    def add_listener(
        self,
        include_value: bool = False,
        item_added_func: typing.Callable[[ItemEvent[ItemType]], None] = None,
        item_removed_func: typing.Callable[[ItemEvent[ItemType]], None] = None,
    ) -> Future[str]:
        """Adds an item listener for this container.

        Listener will be notified for all container add/remove events.

        Args:
            include_value: Whether received events include the updated item or
                not.
            item_added_func: Function to be called when an item is added to
                this set.
            item_removed_func: Function to be called when an item is deleted
                from this set.

        Returns:
            A registration id which is used as a key to remove the listener.
        """
        request = set_add_listener_codec.encode_request(self.name, include_value, self._is_smart)

        def handle_event_item(item_data, uuid, event_type):
            item = self._to_object(item_data) if include_value else None
            member = self._context.cluster_service.get_member(uuid)

            item_event = ItemEvent(self.name, item, event_type, member)
            if event_type == ItemEventType.ADDED:
                if item_added_func:
                    item_added_func(item_event)
            else:
                if item_removed_func:
                    item_removed_func(item_event)

        return self._register_listener(
            request,
            lambda r: set_add_listener_codec.decode_response(r),
            lambda reg_id: set_remove_listener_codec.encode_request(self.name, reg_id),
            lambda m: set_add_listener_codec.handle(m, handle_event_item),
        )

    def clear(self) -> Future[None]:
        """Clears the set. Set will be empty with this call."""
        request = set_clear_codec.encode_request(self.name)
        return self._invoke(request)

    def contains(self, item: ItemType) -> Future[bool]:
        """Determines whether this set contains the specified item or not.

        Args:
            item: The specified item to be searched.

        Returns:
            ``True`` if the specified item exists in this set, ``False``
            otherwise.
        """
        check_not_none(item, "Value can't be None")
        try:
            item_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.contains, item)

        request = set_contains_codec.encode_request(self.name, item_data)
        return self._invoke(request, set_contains_codec.decode_response)

    def contains_all(self, items: typing.Sequence[ItemType]) -> Future[bool]:
        """Determines whether this set contains all items in the specified
        collection or not.

        Args:
            items: The specified collection which includes the items to be
                searched.

        Returns:
            ``True`` if all the items in the specified collection exist in
            this set, ``False`` otherwise.
        """
        check_not_none(items, "Value can't be None")
        try:
            data_items = []
            for item in items:
                check_not_none(item, "Value can't be None")
                data_items.append(self._to_data(item))
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.contains_all, items)

        request = set_contains_all_codec.encode_request(self.name, data_items)
        return self._invoke(request, set_contains_all_codec.decode_response)

    def get_all(self) -> Future[typing.List[ItemType]]:
        """Returns all the items in the set.

        Returns:
            List of the items in this set.
        """

        def handler(message):
            return ImmutableLazyDataList(
                set_get_all_codec.decode_response(message), self._to_object
            )

        request = set_get_all_codec.encode_request(self.name)
        return self._invoke(request, handler)

    def is_empty(self) -> Future[bool]:
        """Determines whether this set is empty or not.

        Returns:
            ``True`` if this set is empty, ``False`` otherwise.
        """
        request = set_is_empty_codec.encode_request(self.name)
        return self._invoke(request, set_is_empty_codec.decode_response)

    def remove(self, item: ItemType) -> Future[bool]:
        """Removes the specified element from the set if it exists.

        Args:
            item: The specified element to be removed.

        Returns:
            ``True`` if the specified element exists in this set, ``False``
            otherwise.
        """
        check_not_none(item, "Value can't be None")
        try:
            item_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.remove, item)

        request = set_remove_codec.encode_request(self.name, item_data)
        return self._invoke(request, set_remove_codec.decode_response)

    def remove_all(self, items: typing.Sequence[ItemType]) -> Future[bool]:
        """Removes all of the elements of the specified collection from this
        set.

        Args:
            items: The specified collection.

        Returns:
            ``True`` if the call changed this set, ``False`` otherwise.
        """
        check_not_none(items, "Value can't be None")
        try:
            data_items = []
            for item in items:
                check_not_none(item, "Value can't be None")
                data_items.append(self._to_data(item))
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.remove_all, items)

        request = set_compare_and_remove_all_codec.encode_request(self.name, data_items)
        return self._invoke(request, set_compare_and_remove_all_codec.decode_response)

    def remove_listener(self, registration_id: str) -> Future[bool]:
        """Removes the specified item listener.

        Returns silently if the specified listener was not added before.

        Args:
            registration_id: Id of the listener to be deleted.

        Returns:
            ``True`` if the item listener is removed, ``False`` otherwise.
        """
        return self._deregister_listener(registration_id)

    def retain_all(self, items: typing.Sequence[ItemType]) -> Future[bool]:
        """Removes the items which are not contained in the specified
        collection.

        In other words, only the items that are contained in the specified
        collection will be retained.

        Args:
            items: Collection which includes the elements to be retained in
                this set.

        Returns:
            ``True`` if this set changed as a result of the call, ``False``
            otherwise.
        """
        check_not_none(items, "Value can't be None")
        try:
            data_items = []
            for item in items:
                check_not_none(item, "Value can't be None")
                data_items.append(self._to_data(item))
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.retain_all, items)

        request = set_compare_and_retain_all_codec.encode_request(self.name, data_items)
        return self._invoke(request, set_compare_and_retain_all_codec.decode_response)

    def size(self) -> Future[int]:
        """Returns the number of items in this set.

        Returns:
            Number of items in this set.
        """
        request = set_size_codec.encode_request(self.name)
        return self._invoke(request, set_size_codec.decode_response)

    def blocking(self) -> "BlockingSet[ItemType]":
        return BlockingSet(self)


class BlockingSet(Set[ItemType]):
    __slots__ = ("_wrapped", "name", "service_name")

    def __init__(self, wrapped: Set[ItemType]):
        self.name = wrapped.name
        self.service_name = wrapped.service_name
        self._wrapped = wrapped

    def add(  # type: ignore[override]
        self,
        item: ItemType,
    ) -> bool:
        return self._wrapped.add(item).result()

    def add_all(  # type: ignore[override]
        self,
        items: typing.Sequence[ItemType],
    ) -> bool:
        return self._wrapped.add_all(items).result()

    def add_listener(  # type: ignore[override]
        self,
        include_value: bool = False,
        item_added_func: typing.Callable[[ItemEvent[ItemType]], None] = None,
        item_removed_func: typing.Callable[[ItemEvent[ItemType]], None] = None,
    ) -> str:
        return self._wrapped.add_listener(
            include_value, item_added_func, item_removed_func
        ).result()

    def clear(  # type: ignore[override]
        self,
    ) -> None:
        return self._wrapped.clear().result()

    def contains(  # type: ignore[override]
        self,
        item: ItemType,
    ) -> bool:
        return self._wrapped.contains(item).result()

    def contains_all(  # type: ignore[override]
        self,
        items: typing.Sequence[ItemType],
    ) -> bool:
        return self._wrapped.contains_all(items).result()

    def get_all(  # type: ignore[override]
        self,
    ) -> typing.List[ItemType]:
        return self._wrapped.get_all().result()

    def is_empty(  # type: ignore[override]
        self,
    ) -> bool:
        return self._wrapped.is_empty().result()

    def remove(  # type: ignore[override]
        self,
        item: ItemType,
    ) -> bool:
        return self._wrapped.remove(item).result()

    def remove_all(  # type: ignore[override]
        self,
        items: typing.Sequence[ItemType],
    ) -> bool:
        return self._wrapped.remove_all(items).result()

    def remove_listener(  # type: ignore[override]
        self,
        registration_id: str,
    ) -> bool:
        return self._wrapped.remove_listener(registration_id).result()

    def retain_all(  # type: ignore[override]
        self,
        items: typing.Sequence[ItemType],
    ) -> bool:
        return self._wrapped.retain_all(items).result()

    def size(  # type: ignore[override]
        self,
    ) -> int:
        return self._wrapped.size().result()

    def blocking(self) -> "BlockingSet[ItemType]":
        return self

    def destroy(self) -> bool:
        return self._wrapped.destroy()

    def __repr__(self) -> str:
        return self._wrapped.__repr__()
