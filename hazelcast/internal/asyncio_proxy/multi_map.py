import asyncio
import typing

from collections import defaultdict

from hazelcast.protocol.codec import (
    multi_map_add_entry_listener_codec,
    multi_map_add_entry_listener_to_key_codec,
    multi_map_clear_codec,
    multi_map_contains_entry_codec,
    multi_map_contains_key_codec,
    multi_map_contains_value_codec,
    multi_map_entry_set_codec,
    multi_map_get_codec,
    multi_map_key_set_codec,
    multi_map_put_codec,
    multi_map_put_all_codec,
    multi_map_remove_codec,
    multi_map_remove_entry_codec,
    multi_map_remove_entry_listener_codec,
    multi_map_size_codec,
    multi_map_value_count_codec,
    multi_map_values_codec,
)
from hazelcast.internal.asyncio_proxy.base import Proxy, EntryEvent, EntryEventType
from hazelcast.types import ValueType, KeyType
from hazelcast.serialization.data import Data
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import (
    check_not_none,
    deserialize_list_in_place,
    deserialize_entry_list_in_place,
)

EntryEventCallable = typing.Callable[[EntryEvent[KeyType, ValueType]], None]

default_thread_id = 0

class MultiMap(Proxy, typing.Generic[KeyType, ValueType]):
    """A specialized map whose keys can be associated with multiple values.

    Example:
        >>> my_map = await client.get_multi_map("my_map")
        >>> print("put", await my_map.put("key", "value1"))
        >>> print("get", await my_map.get("key"))

    Warning:
        Asyncio client multi map proxy is not thread-safe, do not access it from other threads.
    """

    def __init__(self, service_name, name, context):
        super(MultiMap, self).__init__(service_name, name, context)

    async def add_entry_listener(
        self,
        include_value: bool = False,
        key: KeyType = None,
        added_func: EntryEventCallable = None,
        removed_func: EntryEventCallable = None,
        clear_all_func: EntryEventCallable = None,
    ) -> str:
        """Adds an entry listener for this multimap.

        The listener will be notified for all multimap add/remove/clear-all
        events.

        Args:
            include_value: Whether received event should include the value or
                not.
            key: Key for filtering the events.
            added_func: Function to be called when an entry is added to map.
            removed_func: Function to be called when an entry is removed from
                map.
            clear_all_func: Function to be called when entries are cleared
                from map.

        Returns:
            A registration id which is used as a key to remove the listener.
        """
        if key is not None:
            try:
                key_data = self._to_data(key)
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(
                    e,
                    self.add_entry_listener,
                    include_value,
                    key,
                    added_func,
                    removed_func,
                    clear_all_func,
                )

            with_key_codec = multi_map_add_entry_listener_to_key_codec
            request = with_key_codec.encode_request(self.name, key_data, include_value, False)
            response_decoder = with_key_codec.decode_response
            event_message_handler = with_key_codec.handle
        else:
            codec = multi_map_add_entry_listener_codec
            request = codec.encode_request(self.name, include_value, False)
            response_decoder = codec.decode_response
            event_message_handler = codec.handle

        def handle_event_entry(
            key_data,
            value_data,
            old_value_data,
            merging_value_data,
            event_type,
            uuid,
            number_of_affected_entries,
        ):
            event = EntryEvent(
                self._to_object(key_data),
                self._to_object(value_data),
                self._to_object(old_value_data),
                self._to_object(merging_value_data),
                event_type,
                uuid,
                number_of_affected_entries,
            )
            if event.event_type == EntryEventType.ADDED:
                if added_func:
                    added_func(event)
            elif event.event_type == EntryEventType.REMOVED:
                if removed_func:
                    removed_func(event)
            elif event.event_type == EntryEventType.CLEAR_ALL:
                if clear_all_func:
                    clear_all_func(event)

        return await self._register_listener(
            request,
            lambda r: response_decoder(r),
            lambda reg_id: multi_map_remove_entry_listener_codec.encode_request(self.name, reg_id),
            lambda m: event_message_handler(m, handle_event_entry),
        )

    async def contains_key(self, key: KeyType) -> bool:
        """Determines whether this multimap contains an entry with the key.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: The specified key.

        Returns:
            ``True`` if this multimap contains an entry for the specified key,
            ``False`` otherwise.
        """
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.contains_key, key)

        request = multi_map_contains_key_codec.encode_request(self.name, key_data, default_thread_id)
        return await self._invoke_on_key(
            request, key_data, multi_map_contains_key_codec.decode_response
        )

    async def contains_value(self, value: ValueType) -> bool:
        """Determines whether this map contains one or more keys for the
        specified value.

        Args:
            value: The specified value.

        Returns:
            ``True`` if this multimap contains an entry for the specified
            value, ``False`` otherwise.
        """
        check_not_none(value, "value can't be None")
        try:
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.contains_value, value)

        request = multi_map_contains_value_codec.encode_request(self.name, value_data)
        return await self._invoke(request, multi_map_contains_value_codec.decode_response)

    async def contains_entry(self, key: KeyType, value: ValueType) -> bool:
        """Returns whether the multimap contains an entry with the value.

        Args:
            key: The specified key.
            value: The specified value.

        Returns:
            ``True`` if this multimap contains the key-value tuple, ``False``
            otherwise.
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.contains_entry, key, value)

        request = multi_map_contains_entry_codec.encode_request(self.name, key_data, value_data,  default_thread_id)
        return await self._invoke_on_key(
            request, key_data, multi_map_contains_entry_codec.decode_response
        )

    async def clear(self) -> None:
        """Clears the multimap. Removes all key-value tuples."""
        request = multi_map_clear_codec.encode_request(self.name)
        return await self._invoke(request)

    async def entry_set(self) -> typing.List[typing.Tuple[KeyType, ValueType]]:
        """Returns the list of key-value tuples in the multimap.

        Warning:
            The list is NOT backed by the map, so changes to the map are NOT
            reflected in the list, and vice-versa.

        Returns:
            The list of key-value tuples in the multimap.
        """

        def handler(message):
            entry_data_list = multi_map_entry_set_codec.decode_response(message)
            return deserialize_entry_list_in_place(entry_data_list, self._to_object)

        request = multi_map_entry_set_codec.encode_request(self.name)
        return await self._invoke(request, handler)

    async def get(self, key: KeyType) -> typing.Optional[typing.List[ValueType]]:
        """Returns the list of values associated with the key. ``None`` if
        this map does not contain this key.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` of the binary form of
            the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in the key's class.

        Warning:
            The list is NOT backed by the multimap, so changes to the map are
            not reflected in the collection, and vice-versa.

        Args:
            key: The specified key.

        Returns:
            The list of the values associated with the specified key.
        """
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.get, key)

        def handler(message):
            data_list = multi_map_get_codec.decode_response(message)
            return deserialize_list_in_place(data_list, self._to_object)

        request = multi_map_get_codec.encode_request(self.name, key_data, default_thread_id)
        return await self._invoke_on_key(request, key_data, handler)

    async def key_set(self) -> typing.List[KeyType]:
        """Returns the list of keys in the multimap.

        Warning:
            The list is NOT backed by the map, so changes to the map are NOT
            reflected in the list, and vice-versa.

        Returns:
            A list of the clone of the keys.
        """

        def handler(message):
            data_list = multi_map_key_set_codec.decode_response(message)
            return deserialize_list_in_place(data_list, self._to_object)

        request = multi_map_key_set_codec.encode_request(self.name)
        return await self._invoke(request, handler)

    async def remove(self, key: KeyType, value: ValueType) -> bool:
        """Removes the given key-value tuple from the multimap.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: The key of the entry to remove.
            value: The value of the entry to remove.

        Returns:
            ``True`` if the size of the multimap changed after the remove
            operation, ``False`` otherwise.
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.remove, key, value)

        request = multi_map_remove_entry_codec.encode_request(self.name, key_data, value_data, default_thread_id)
        return await self._invoke_on_key(
            request, key_data, multi_map_remove_entry_codec.decode_response
        )

    async def remove_all(self, key: KeyType) -> typing.List[ValueType]:
        """Removes all the entries with the given key and returns the value
        list associated with this key.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Warning:
            The returned list is NOT backed by the map, so changes to the map
            are NOT reflected in the list, and vice-versa.

        Args:
            key: The key of the entries to remove.

        Returns:
            The collection of removed values associated with the given key.
        """
        check_not_none(key, "key can't be None")

        def handler(message):
            data_list = multi_map_remove_codec.decode_response(message)
            return deserialize_list_in_place(data_list, self._to_object)

        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.remove_all, key)

        request = multi_map_remove_codec.encode_request(self.name, key_data, default_thread_id)
        return await self._invoke_on_key(request, key_data, handler)

    async def put(self, key: KeyType, value: ValueType) -> bool:
        """Stores a key-value tuple in the multimap.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: The key to be stored.
            value: The value to be stored.

        Returns:
            ``True`` if size of the multimap is increased, ``False`` if the
            multimap already contains the key-value tuple.
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.put, key, value)

        request = multi_map_put_codec.encode_request(self.name, key_data, value_data, default_thread_id)
        return await self._invoke_on_key(request, key_data, multi_map_put_codec.decode_response)

    async def put_all(self, multimap: typing.Dict[KeyType, typing.Sequence[ValueType]]) -> None:
        """Stores the given Map in the MultiMap.

        The results of concurrently mutating the given map are undefined.
        No atomicity guarantees are given. It could be that in case of failure some of the key/value-pairs get written, while others are not.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            multimap: the map corresponds to multimap entries.
        """
        check_not_none(multimap, "multimap can't be None")
        if not multimap:
            return None

        partition_service = self._context.partition_service
        partition_map: typing.DefaultDict[
            int, typing.List[typing.Tuple[Data, typing.List[Data]]]
        ] = defaultdict(list)

        for key, values in multimap.items():
            try:
                check_not_none(key, "key can't be None")
                check_not_none(values, "values can't be None")
                serialized_key = self._to_data(key)
                serialized_values = []
                for value in values:
                    check_not_none(value, "value can't be None")
                    serialized_values.append(self._to_data(value))
                partition_id = partition_service.get_partition_id(serialized_key)
                partition_map[partition_id].append((serialized_key, serialized_values))
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(e, self.put_all, multimap)

        async with asyncio.TaskGroup() as tg:  # type: ignore[attr-defined]
            for partition_id, entry_list in partition_map.items():
                request = multi_map_put_all_codec.encode_request(self.name, entry_list)
                tg.create_task(self._ainvoke_on_partition(request, partition_id))

        return None

    async def remove_entry_listener(self, registration_id: str) -> bool:
        """Removes the specified entry listener.

        Returns silently if there is no such listener added before.

        Args:
            registration_id: Id of registered listener.

        Returns:
            ``True`` if registration is removed, ``False`` otherwise.
        """
        return await self._deregister_listener(registration_id)

    async def size(self) -> int:
        """Returns the number of entries in this multimap.

        Returns:
            Number of entries in this multimap.
        """
        request = multi_map_size_codec.encode_request(self.name)
        return await self._invoke(request, multi_map_size_codec.decode_response)

    async def value_count(self, key: KeyType) -> int:
        """Returns the number of values that match the given key in the
        multimap.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: The key whose values count is to be returned.

        Returns:
            The number of values that match the given key in the multimap.
        """
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.value_count, key)

        request = multi_map_value_count_codec.encode_request(self.name, key_data, default_thread_id)
        return await self._invoke_on_key(
            request, key_data, multi_map_value_count_codec.decode_response
        )

    async def values(self) -> typing.List[ValueType]:
        """Returns the list of values in the multimap.

        Warning:
            The returned list is NOT backed by the map, so changes to the map
            are NOT reflected in the list, and vice-versa.

        Returns:
            The list of values in the multimap.
        """

        def handler(message):
            data_list = multi_map_values_codec.decode_response(message)
            return deserialize_list_in_place(data_list, self._to_object)

        request = multi_map_values_codec.encode_request(self.name)
        return await self._invoke(request, handler)


async def create_multi_map_proxy(service_name, name, context):
    return MultiMap(service_name, name, context)
