import typing

from hazelcast.protocol.codec import (
    list_add_all_codec,
    list_add_all_with_index_codec,
    list_add_codec,
    list_add_listener_codec,
    list_add_with_index_codec,
    list_clear_codec,
    list_compare_and_remove_all_codec,
    list_compare_and_retain_all_codec,
    list_contains_all_codec,
    list_contains_codec,
    list_get_all_codec,
    list_get_codec,
    list_index_of_codec,
    list_is_empty_codec,
    list_iterator_codec,
    list_last_index_of_codec,
    list_list_iterator_codec,
    list_remove_codec,
    list_remove_listener_codec,
    list_remove_with_index_codec,
    list_set_codec,
    list_size_codec,
    list_sub_codec,
)
from hazelcast.internal.asyncio_proxy.base import (
    PartitionSpecificProxy,
    ItemEvent,
    ItemEventType,
)
from hazelcast.types import ItemType
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import check_not_none, deserialize_list_in_place


class List(PartitionSpecificProxy, typing.Generic[ItemType]):
    """Concurrent, distributed implementation of List.

    The Hazelcast List is not a partitioned data-structure. So all the content
    of the List is stored in a single machine (and in the backup). So the List
    will not scale by adding more members in the cluster.

    Example:
        >>> my_list = await client.get_list("my_list")
        >>> print("list.add", await my_list.add("item"))
        >>> print("list.size", await my_list.size())

    Warning:
        Asyncio client list proxy is not thread-safe, do not access it from other threads.
    """

    async def add(self, item: ItemType) -> bool:
        """Adds the specified item to the end of this list.

        Args:
            item: the specified item to be appended to this list.

        Returns:
            ``True`` if item is added, ``False`` otherwise.
        """
        check_not_none(item, "Value can't be None")
        try:
            element_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.add, item)

        request = list_add_codec.encode_request(self.name, element_data)
        return await self._invoke(request, list_add_codec.decode_response)

    async def add_at(self, index: int, item: ItemType) -> None:
        """Adds the specified item at the specific position in this list.
        Element in this position and following elements are shifted to the
        right, if any.

        Args:
            index: The specified index to insert the item.
            item: The specified item to be inserted.
        """
        check_not_none(item, "Value can't be None")
        try:
            element_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.add_at, index, item)

        request = list_add_with_index_codec.encode_request(self.name, index, element_data)
        return await self._invoke(request)

    async def add_all(self, items: typing.Sequence[ItemType]) -> bool:
        """Adds all of the items in the specified collection to the end of this
        list.

        The order of new elements is determined by the specified collection's
        iterator.

        Args:
            items: The specified collection which includes the elements to be
                added to list.

        Returns:
            ``True`` if this call changed the list, ``False`` otherwise.
        """
        check_not_none(items, "Value can't be None")

        try:
            data_items = []
            for item in items:
                check_not_none(item, "Value can't be None")
                data_items.append(self._to_data(item))
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.add_all, items)

        request = list_add_all_codec.encode_request(self.name, data_items)
        return await self._invoke(request, list_add_all_codec.decode_response)

    async def add_all_at(self, index: int, items: typing.Sequence[ItemType]) -> bool:
        """Adds all of the elements in the specified collection into this list
        at the specified position.

        Elements in this positions and following elements are shifted to the
        right, if any. The order of new elements is determined by the specified
        collection's iterator.

        Args:
            index: The specified index at which the first element of specified
                collection is added.
            items: The specified collection which includes the elements to be
                added to list.

        Returns:
            ``True`` if this call changed the list, ``False`` otherwise.
        """
        check_not_none(items, "Value can't be None")
        try:
            data_items = []
            for item in items:
                check_not_none(item, "Value can't be None")
                data_items.append(self._to_data(item))
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.add_all_at, index, items)

        request = list_add_all_with_index_codec.encode_request(self.name, index, data_items)
        return await self._invoke(request, list_add_all_with_index_codec.decode_response)

    async def add_listener(
        self,
        include_value: bool = False,
        item_added_func: typing.Callable[[ItemEvent[ItemType]], None] = None,
        item_removed_func: typing.Callable[[ItemEvent[ItemType]], None] = None,
    ) -> str:
        """Adds an item listener for this list. Listener will be notified for
        all list add/remove events.

        Args:
            include_value: Whether received events include the updated item or
                not.
            item_added_func: To be called when an item is added to this list.
            item_removed_func: To be called when an item is deleted from this
                list.

        Returns:
            A registration id which is used as a key to remove the listener.
        """
        request = list_add_listener_codec.encode_request(self.name, include_value, self._is_smart)

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

        return await self._register_listener(
            request,
            lambda r: list_add_listener_codec.decode_response(r),
            lambda reg_id: list_remove_listener_codec.encode_request(self.name, reg_id),
            lambda m: list_add_listener_codec.handle(m, handle_event_item),
        )

    async def clear(self) -> None:
        """Clears the list.

        List will be empty with this call.
        """
        request = list_clear_codec.encode_request(self.name)
        return await self._invoke(request)

    async def contains(self, item: ItemType) -> bool:
        """Determines whether this list contains the specified item or not.

        Args:
            item: The specified item.

        Returns:
            ``True`` if the specified item exists in this list, ``False``
            otherwise.
        """
        check_not_none(item, "Value can't be None")
        try:
            item_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.contains, item)

        request = list_contains_codec.encode_request(self.name, item_data)
        return await self._invoke(request, list_contains_codec.decode_response)

    async def contains_all(self, items: typing.Sequence[ItemType]) -> bool:
        """Determines whether this list contains all of the items in specified
        collection or not.

        Args:
            items: The specified collection which includes the items to be
                searched.

        Returns:
            ``True`` if all of the items in specified collection exist in this
            list, ``False`` otherwise.
        """
        check_not_none(items, "Items can't be None")
        try:
            data_items = []
            for item in items:
                check_not_none(item, "item can't be None")
                data_items.append(self._to_data(item))
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.contains_all, items)

        request = list_contains_all_codec.encode_request(self.name, data_items)
        return await self._invoke(request, list_contains_all_codec.decode_response)

    async def get(self, index: int) -> ItemType:
        """Returns the item which is in the specified position in this list.

        Args:
            index: the specified index of the item to be returned.

        Returns:
            The item in the specified position in this list.
        """

        def handler(message):
            return self._to_object(list_get_codec.decode_response(message))

        request = list_get_codec.encode_request(self.name, index)
        return await self._invoke(request, handler)

    async def get_all(self) -> typing.List[ItemType]:
        """Returns all the items in this list.

        Returns:
            All the items in this list.
        """

        def handler(message):
            data_list = list_get_all_codec.decode_response(message)
            return deserialize_list_in_place(data_list, self._to_object)

        request = list_get_all_codec.encode_request(self.name)
        return await self._invoke(request, handler)

    async def iterator(self) -> typing.List[ItemType]:
        """Returns an iterator over the elements in this list in proper
        sequence, same with ``get_all``.

        Returns:
            All the items in this list.
        """

        def handler(message):
            data_list = list_iterator_codec.decode_response(message)
            return deserialize_list_in_place(data_list, self._to_object)

        request = list_iterator_codec.encode_request(self.name)
        return await self._invoke(request, handler)

    async def index_of(self, item: ItemType) -> int:
        """Returns the first index of specified item's occurrences in this
        list.

        If specified item is not present in this list, returns -1.

        Args:
            item: The specified item to be searched for.

        Returns:
             The first index of specified item's occurrences, ``-1`` if item
             is not present in this list.
        """
        check_not_none(item, "Value can't be None")
        try:
            item_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.index_of, item)

        request = list_index_of_codec.encode_request(self.name, item_data)
        return await self._invoke(request, list_index_of_codec.decode_response)

    async def is_empty(self) -> bool:
        """Determines whether this list is empty or not.

        Returns:
            ``True`` if the list contains no elements, ``False`` otherwise.
        """
        request = list_is_empty_codec.encode_request(self.name)
        return await self._invoke(request, list_is_empty_codec.decode_response)

    async def last_index_of(self, item: ItemType) -> int:
        """Returns the last index of specified item's occurrences in this list.

        If specified item is not present in this list, returns -1.

        Args:
            item: The specified item to be searched for.

        Returns:
            The last index of specified item's occurrences, ``-1`` if item is
            not present in this list.
        """
        check_not_none(item, "Value can't be None")
        try:
            item_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.last_index_of, item)

        request = list_last_index_of_codec.encode_request(self.name, item_data)
        return await self._invoke(request, list_last_index_of_codec.decode_response)

    async def list_iterator(self, index: int = 0) -> typing.List[ItemType]:
        """Returns a list iterator of the elements in this list.

        If an index is provided, iterator starts from this index.

        Args:
            index: Index of first element to be returned from the list
                iterator.

        Returns:
            List of the elements in this list.
        """

        def handler(message):
            data_list = list_list_iterator_codec.decode_response(message)
            return deserialize_list_in_place(data_list, self._to_object)

        request = list_list_iterator_codec.encode_request(self.name, index)
        return await self._invoke(request, handler)

    async def remove(self, item: ItemType) -> bool:
        """Removes the specified element's first occurrence from the list if it
        exists in this list.

        Args:
            item: The specified element.

        Returns:
            ``True`` if the specified element is present in this list,
            ``False`` otherwise.
        """
        check_not_none(item, "Value can't be None")
        try:
            item_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.remove, item)

        request = list_remove_codec.encode_request(self.name, item_data)
        return await self._invoke(request, list_remove_codec.decode_response)

    async def remove_at(self, index: int) -> ItemType:
        """Removes the item at the specified position in this list.

        Element in this position and following elements are shifted to the
        left, if any.

        Args:
            index: Index of the item to be removed.

        Returns:
            The item previously at the specified index.
        """

        def handler(message):
            return self._to_object(list_remove_with_index_codec.decode_response(message))

        request = list_remove_with_index_codec.encode_request(self.name, index)
        return await self._invoke(request, handler)

    async def remove_all(self, items: typing.Sequence[ItemType]) -> bool:
        """Removes all of the elements that is present in the specified
        collection from this list.

        Args:
            items: The specified collection.

        Returns:
            ``True`` if this list changed as a result of the call,
            ``False`` otherwise.
        """
        check_not_none(items, "Value can't be None")
        try:
            data_items = []
            for item in items:
                check_not_none(item, "Value can't be None")
                data_items.append(self._to_data(item))
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.remove_all, items)

        request = list_compare_and_remove_all_codec.encode_request(self.name, data_items)
        return await self._invoke(request, list_compare_and_remove_all_codec.decode_response)

    async def remove_listener(self, registration_id: str) -> bool:
        """Removes the specified item listener.

        Returns silently if the specified listener was not added before.

        Args:
            registration_id: Id of the listener to be deleted.

        Returns:
            ``True`` if the item listener is removed, ``False`` otherwise.
        """
        return await self._deregister_listener(registration_id)

    async def retain_all(self, items: typing.Sequence[ItemType]) -> bool:
        """Retains only the items that are contained in the specified
        collection.

        It means, items which are not present in the specified collection are
        removed from this list.

        Args:
            items: Collections which includes the elements to be retained in
                this list.

        Returns:
            ``True`` if this list changed as a result of the call, ``False``
            otherwise.
        """
        check_not_none(items, "Value can't be None")
        try:
            data_items = []
            for item in items:
                check_not_none(item, "Value can't be None")
                data_items.append(self._to_data(item))
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.retain_all, items)

        request = list_compare_and_retain_all_codec.encode_request(self.name, data_items)
        return await self._invoke(request, list_compare_and_retain_all_codec.decode_response)

    async def size(self) -> int:
        """Returns the number of elements in this list.

        Returns:
            Number of elements in this list.
        """
        request = list_size_codec.encode_request(self.name)
        return await self._invoke(request, list_size_codec.decode_response)

    async def set_at(self, index: int, item: ItemType) -> ItemType:
        """Replaces the specified element with the element at the specified
        position in this list.

        Args:
            index: Index of the item to be replaced.
            item: Item to be stored.

        Returns:
            The previous item in the specified index.
        """
        check_not_none(item, "Value can't be None")
        try:
            element_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.set_at, index, item)

        def handler(message):
            return self._to_object(list_set_codec.decode_response(message))

        request = list_set_codec.encode_request(self.name, index, element_data)
        return await self._invoke(request, handler)

    async def sub_list(self, from_index: int, to_index: int) -> typing.List[ItemType]:
        """Returns a sublist from this list, from from_index(inclusive) to
        to_index(exclusive).

        The returned list is backed by this list, so non-structural changes in
        the returned list are reflected in this list, and vice-versa.

        Args:
            from_index: The start point(inclusive) of the sub_list.
            to_index: The end point(exclusive) of the sub_list.

        Returns:
            A view of the specified range within this list.
        """

        def handler(message):
            data_list = list_sub_codec.decode_response(message)
            return deserialize_list_in_place(data_list, self._to_object)

        request = list_sub_codec.encode_request(self.name, from_index, to_index)
        return await self._invoke(request, handler)


async def create_list_proxy(service_name, name, context):
    return List(service_name, name, context)
