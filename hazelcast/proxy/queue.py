import typing

from hazelcast.errors import IllegalStateError
from hazelcast.future import Future
from hazelcast.protocol.codec import (
    queue_add_all_codec,
    queue_add_listener_codec,
    queue_clear_codec,
    queue_compare_and_remove_all_codec,
    queue_compare_and_retain_all_codec,
    queue_contains_all_codec,
    queue_contains_codec,
    queue_drain_to_max_size_codec,
    queue_is_empty_codec,
    queue_iterator_codec,
    queue_offer_codec,
    queue_peek_codec,
    queue_poll_codec,
    queue_put_codec,
    queue_remaining_capacity_codec,
    queue_remove_codec,
    queue_remove_listener_codec,
    queue_size_codec,
    queue_take_codec,
)
from hazelcast.proxy.base import PartitionSpecificProxy, ItemEvent, ItemEventType
from hazelcast.types import ItemType
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import check_not_none, to_millis, ImmutableLazyDataList


class Queue(PartitionSpecificProxy["BlockingQueue"], typing.Generic[ItemType]):
    """Concurrent, blocking, distributed, observable queue.

    Queue is not a partitioned data-structure. All of the Queue content is
    stored in a single machine (and in the backup). Queue will not scale by
    adding more members in the cluster.
    """

    def add(self, item: ItemType) -> Future[bool]:
        """Adds the specified item to this queue if there is available space.

        Args:
            item: The specified item.

        Returns:
            ``True`` if element is successfully added, ``False`` otherwise.
        """

        def result_fnc(f):
            if f.result():
                return True
            raise IllegalStateError("Queue is full!")

        return self.offer(item).continue_with(result_fnc)

    def add_all(self, items: typing.Sequence[ItemType]) -> Future[bool]:
        """Adds the elements in the specified collection to this queue.

        Args:
            items: Collection which includes the items to be added.

        Returns:
            ``True`` if this queue is changed after call, ``False`` otherwise.
        """
        check_not_none(items, "Value can't be None")
        try:
            data_items = []
            for item in items:
                check_not_none(item, "Value can't be None")
                data_items.append(self._to_data(item))
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.add_all, items)

        request = queue_add_all_codec.encode_request(self.name, data_items)
        return self._invoke(request, queue_add_all_codec.decode_response)

    def add_listener(
        self,
        include_value: bool = False,
        item_added_func: typing.Callable[[ItemEvent[ItemType]], None] = None,
        item_removed_func: typing.Callable[[ItemEvent[ItemType]], None] = None,
    ) -> Future[str]:
        """Adds an item listener for this queue. Listener will be notified for
         all queue add/remove events.

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
        codec = queue_add_listener_codec
        request = codec.encode_request(self.name, include_value, self._is_smart)

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
            lambda r: codec.decode_response(r),
            lambda reg_id: queue_remove_listener_codec.encode_request(self.name, reg_id),
            lambda m: codec.handle(m, handle_event_item),
        )

    def clear(self) -> Future[None]:
        """Clears this queue. Queue will be empty after this call."""
        request = queue_clear_codec.encode_request(self.name)
        return self._invoke(request)

    def contains(self, item: ItemType) -> Future[bool]:
        """Determines whether this queue contains the specified item or not.

        Args:
            item: The specified item to be searched.

        Returns:
            ``True`` if the specified item exists in this queue, ``False``
            otherwise.
        """
        check_not_none(item, "Item can't be None")
        try:
            item_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.contains, item)

        request = queue_contains_codec.encode_request(self.name, item_data)
        return self._invoke(request, queue_contains_codec.decode_response)

    def contains_all(self, items: typing.Sequence[ItemType]) -> Future[bool]:
        """Determines whether this queue contains all of the items in the
        specified collection or not.

        Args:
            items: The specified collection which includes the items to be
                searched.

        Returns:
            ``True`` if all of the items in the specified collection exist in
            this queue, ``False`` otherwise.
        """
        check_not_none(items, "Items can't be None")
        try:
            data_items = []
            for item in items:
                check_not_none(item, "item can't be None")
                data_items.append(self._to_data(item))
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.contains_all, items)

        request = queue_contains_all_codec.encode_request(self.name, data_items)
        return self._invoke(request, queue_contains_all_codec.decode_response)

    def drain_to(self, target_list: typing.List[ItemType], max_size: int = -1) -> Future[int]:
        """Transfers all available items to the given `target_list` and removes
        these items from this queue.

        If a max_size is specified, it transfers at most the given number of
        items. In case of a failure, an item can exist in both collections or
        none of them.

        This operation may be more efficient than polling elements repeatedly
        and putting into collection.

        Args:
            target_list: the list where the items in this queue will be
                transferred.
            max_size: The maximum number items to transfer.

        Returns:
            Number of transferred items.
        """

        def handler(message):
            response = queue_drain_to_max_size_codec.decode_response(message)
            items = [self._to_object(item) for item in response]
            target_list.extend(items)
            return len(response)

        request = queue_drain_to_max_size_codec.encode_request(self.name, max_size)
        return self._invoke(request, handler)

    def iterator(self) -> Future[typing.List[ItemType]]:
        """Returns all the items in this queue.

        Returns:
            Collection of items in this queue.
        """

        def handler(message):
            return ImmutableLazyDataList(
                queue_iterator_codec.decode_response(message), self._to_object
            )

        request = queue_iterator_codec.encode_request(self.name)
        return self._invoke(request, handler)

    def is_empty(self) -> Future[bool]:
        """Determines whether this set is empty or not.

        Returns:
            ``True`` if this queue is empty, ``False`` otherwise.
        """
        request = queue_is_empty_codec.encode_request(self.name)
        return self._invoke(request, queue_is_empty_codec.decode_response)

    def offer(self, item: ItemType, timeout: float = 0) -> Future[bool]:
        """Inserts the specified element into this queue if it is possible to
        do so immediately without violating capacity restrictions.

        If there is no space currently available:

        - If the timeout is provided, it waits until this timeout elapses
          and returns the result.
        - If the timeout is not provided, returns ``False`` immediately.

        Args:
            item: The item to be added.
            timeout: Maximum time in seconds to wait for addition.

        Returns:
            ``True`` if the element was added to this queue, ``False``
            otherwise.
        """
        check_not_none(item, "Value can't be None")
        try:
            element_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.offer, item, timeout)

        request = queue_offer_codec.encode_request(self.name, element_data, to_millis(timeout))
        return self._invoke(request, queue_offer_codec.decode_response)

    def peek(self) -> Future[typing.Optional[ItemType]]:
        """Retrieves the head of queue without removing it from the queue.

        Returns:
            The head of this queue, or ``None`` if this queue is empty.
        """

        def handler(message):
            return self._to_object(queue_peek_codec.decode_response(message))

        request = queue_peek_codec.encode_request(self.name)
        return self._invoke(request, handler)

    def poll(self, timeout: float = 0) -> Future[typing.Optional[ItemType]]:
        """Retrieves and removes the head of this queue.

        If this queue is empty:

        - If the timeout is provided, it waits until this timeout elapses
          and returns the result.
        - If the timeout is not provided, returns ``None``.

        Args:
            timeout: Maximum time in seconds to wait for addition.

        Returns:
            The head of this queue, or ``None`` if this queue is empty or
            specified timeout elapses before an item is added to the queue.
        """

        def handler(message):
            return self._to_object(queue_poll_codec.decode_response(message))

        request = queue_poll_codec.encode_request(self.name, to_millis(timeout))
        return self._invoke(request, handler)

    def put(self, item: ItemType) -> Future[None]:
        """Adds the specified element into this queue.

        If there is no space, it waits until necessary space becomes available.

        Args:
            item: The specified item.
        """
        check_not_none(item, "Value can't be None")
        try:
            element_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.put, item)

        request = queue_put_codec.encode_request(self.name, element_data)
        return self._invoke(request)

    def remaining_capacity(self) -> Future[int]:
        """Returns the remaining capacity of this queue.

        Returns:
            Remaining capacity of this queue.
        """
        request = queue_remaining_capacity_codec.encode_request(self.name)
        return self._invoke(request, queue_remaining_capacity_codec.decode_response)

    def remove(self, item: ItemType) -> Future[bool]:
        """Removes the specified element from the queue if it exists.

        Args:
            item: The specified element to be removed.

        Returns:
            ``True`` if the specified element exists in this queue, ``False``
            otherwise.
        """
        check_not_none(item, "Value can't be None")
        try:
            item_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.remove, item)

        request = queue_remove_codec.encode_request(self.name, item_data)
        return self._invoke(request, queue_remove_codec.decode_response)

    def remove_all(self, items: typing.Sequence[ItemType]) -> Future[bool]:
        """Removes all of the elements of the specified collection from this
        queue.

        Args:
            items: The specified collection.

        Returns:
            ``True`` if the call changed this queue, ``False`` otherwise.
        """
        check_not_none(items, "Value can't be None")
        try:
            data_items = []
            for item in items:
                check_not_none(item, "Value can't be None")
                data_items.append(self._to_data(item))
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.remove_all, items)

        request = queue_compare_and_remove_all_codec.encode_request(self.name, data_items)
        return self._invoke(request, queue_compare_and_remove_all_codec.decode_response)

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
            ``True`` if this queue changed as a result of the call, ``False``
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

        request = queue_compare_and_retain_all_codec.encode_request(self.name, data_items)
        return self._invoke(request, queue_compare_and_retain_all_codec.decode_response)

    def size(self) -> Future[int]:
        """Returns the number of elements in this collection.

        If the size is greater than ``2**31 - 1``, it returns ``2**31 - 1``.

        Returns:
            Size of the queue.
        """
        request = queue_size_codec.encode_request(self.name)
        return self._invoke(request, queue_size_codec.decode_response)

    def take(self) -> Future[ItemType]:
        """Retrieves and removes the head of this queue, if necessary, waits
        until an item becomes available.

        Returns:
            The head of this queue.
        """

        def handler(message):
            return self._to_object(queue_take_codec.decode_response(message))

        request = queue_take_codec.encode_request(self.name)
        return self._invoke(request, handler)

    def blocking(self) -> "BlockingQueue[ItemType]":
        return BlockingQueue(self)


class BlockingQueue(Queue[ItemType]):
    __slots__ = ("_wrapped", "name", "service_name")

    def __init__(self, wrapped: Queue[ItemType]):
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

    def drain_to(  # type: ignore[override]
        self,
        target_list: typing.List[ItemType],
        max_size: int = -1,
    ) -> int:
        return self._wrapped.drain_to(target_list, max_size).result()

    def iterator(  # type: ignore[override]
        self,
    ) -> typing.List[ItemType]:
        return self._wrapped.iterator().result()

    def is_empty(  # type: ignore[override]
        self,
    ) -> bool:
        return self._wrapped.is_empty().result()

    def offer(  # type: ignore[override]
        self,
        item: ItemType,
        timeout: float = 0,
    ) -> bool:
        return self._wrapped.offer(item, timeout).result()

    def peek(  # type: ignore[override]
        self,
    ) -> typing.Optional[ItemType]:
        return self._wrapped.peek().result()

    def poll(  # type: ignore[override]
        self,
        timeout: float = 0,
    ) -> typing.Optional[ItemType]:
        return self._wrapped.poll(timeout).result()

    def put(  # type: ignore[override]
        self,
        item: ItemType,
    ) -> None:
        return self._wrapped.put(item).result()

    def remaining_capacity(  # type: ignore[override]
        self,
    ) -> int:
        return self._wrapped.remaining_capacity().result()

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

    def take(  # type: ignore[override]
        self,
    ) -> ItemType:
        return self._wrapped.take().result()

    def destroy(self) -> bool:
        return self._wrapped.destroy()

    def blocking(self) -> "BlockingQueue[ItemType]":
        return self

    def __repr__(self) -> str:
        return self._wrapped.__repr__()
