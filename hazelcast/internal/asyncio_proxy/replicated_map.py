import typing
from random import randint

from hazelcast.predicate import Predicate
from hazelcast.protocol.codec import (
    replicated_map_clear_codec,
    replicated_map_add_entry_listener_codec,
    replicated_map_add_entry_listener_to_key_codec,
    replicated_map_add_entry_listener_to_key_with_predicate_codec,
    replicated_map_add_entry_listener_with_predicate_codec,
    replicated_map_contains_key_codec,
    replicated_map_contains_value_codec,
    replicated_map_entry_set_codec,
    replicated_map_get_codec,
    replicated_map_is_empty_codec,
    replicated_map_key_set_codec,
    replicated_map_put_all_codec,
    replicated_map_put_codec,
    replicated_map_remove_codec,
    replicated_map_remove_entry_listener_codec,
    replicated_map_size_codec,
    replicated_map_values_codec,
)
from hazelcast.internal.asyncio_proxy.base import Proxy, EntryEvent, EntryEventType
from hazelcast.types import KeyType, ValueType
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import (
    to_millis,
    check_not_none,
    deserialize_list_in_place,
    deserialize_entry_list_in_place,
)

EntryEventCallable = typing.Callable[[EntryEvent[KeyType, ValueType]], None]


class ReplicatedMap(Proxy, typing.Generic[KeyType, ValueType]):
    """A ReplicatedMap is a map-like data structure with weak consistency and
    values locally stored on every node of the cluster.

    Whenever a value is written asynchronously, the new value will be
    internally distributed to all existing cluster members, and eventually
    every node will have the new value.

    When a new node joins the cluster, the new node initially will request
    existing values from older nodes and replicate them locally.

    Example:
        >>> my_map = await client.get_replicated_map("my_map")
        >>> print("put", await my_map.put("key", "value"))
        >>> print("get", await my_map.get("key"))
    """

    def __init__(self, service_name, name, context):
        super(ReplicatedMap, self).__init__(service_name, name, context)
        partition_service = context.partition_service
        self._partition_id = randint(0, partition_service.partition_count - 1)

    async def add_entry_listener(
        self,
        key: KeyType = None,
        predicate: Predicate = None,
        added_func: EntryEventCallable = None,
        removed_func: EntryEventCallable = None,
        updated_func: EntryEventCallable = None,
        evicted_func: EntryEventCallable = None,
        clear_all_func: EntryEventCallable = None,
    ) -> str:
        """Adds a continuous entry listener for this map.

        Listener will get notified for map events filtered with given
        parameters.

        Args:
            key: Key for filtering the events.
            predicate: Predicate for filtering the events.
            added_func: Function to be called when an entry is added to map.
            removed_func: Function to be called when an entry is removed from
                map.
            updated_func: Function to be called when an entry is updated.
            evicted_func: Function to be called when an entry is evicted from
                map.
            clear_all_func: Function to be called when entries are cleared
                from map.

        Returns:
            A registration id which is used as a key to remove the listener.
        """
        if key is not None and predicate is not None:
            try:
                key_data = self._to_data(key)
                predicate_data = self._to_data(predicate)
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(
                    e,
                    self.add_entry_listener,
                    key,
                    predicate,
                    added_func,
                    removed_func,
                    updated_func,
                    evicted_func,
                    clear_all_func,
                )

            with_key_and_predicate_codec = (
                replicated_map_add_entry_listener_to_key_with_predicate_codec
            )
            request = with_key_and_predicate_codec.encode_request(
                self.name, key_data, predicate_data, self._is_smart
            )
            response_decoder = with_key_and_predicate_codec.decode_response
            event_message_handler = with_key_and_predicate_codec.handle
        elif key is not None and predicate is None:
            try:
                key_data = self._to_data(key)
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(
                    e,
                    self.add_entry_listener,
                    key,
                    predicate,
                    added_func,
                    removed_func,
                    updated_func,
                    evicted_func,
                    clear_all_func,
                )

            with_key_codec = replicated_map_add_entry_listener_to_key_codec
            request = with_key_codec.encode_request(self.name, key_data, self._is_smart)
            response_decoder = with_key_codec.decode_response
            event_message_handler = with_key_codec.handle
        elif key is None and predicate is not None:
            try:
                predicate_data = self._to_data(predicate)
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(
                    e,
                    self.add_entry_listener,
                    key,
                    predicate,
                    added_func,
                    removed_func,
                    updated_func,
                    evicted_func,
                    clear_all_func,
                )

            with_predicate_codec = replicated_map_add_entry_listener_with_predicate_codec
            request = with_predicate_codec.encode_request(self.name, predicate_data, self._is_smart)
            response_decoder = with_predicate_codec.decode_response
            event_message_handler = with_predicate_codec.handle
        else:
            codec = replicated_map_add_entry_listener_codec
            request = codec.encode_request(self.name, self._is_smart)
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
            if event.event_type == EntryEventType.ADDED and added_func:
                added_func(event)
            elif event.event_type == EntryEventType.REMOVED and removed_func:
                removed_func(event)
            elif event.event_type == EntryEventType.UPDATED and updated_func:
                updated_func(event)
            elif event.event_type == EntryEventType.EVICTED and evicted_func:
                evicted_func(event)
            elif event.event_type == EntryEventType.CLEAR_ALL and clear_all_func:
                clear_all_func(event)

        return await self._register_listener(
            request,
            lambda r: response_decoder(r),
            lambda reg_id: replicated_map_remove_entry_listener_codec.encode_request(
                self.name, reg_id
            ),
            lambda m: event_message_handler(m, handle_event_entry),
        )

    async def clear(self) -> None:
        """Wipes data out of the replicated map."""
        request = replicated_map_clear_codec.encode_request(self.name)
        return await self._invoke(request)

    async def contains_key(self, key: KeyType) -> bool:
        """Determines whether this map contains an entry with the key.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: The specified key.

        Returns:
            ``True`` if this map contains an entry for the specified key,
            ``False`` otherwise.
        """
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.contains_key, key)

        request = replicated_map_contains_key_codec.encode_request(self.name, key_data)
        return await self._invoke_on_key(
            request, key_data, replicated_map_contains_key_codec.decode_response
        )

    async def contains_value(self, value: ValueType) -> bool:
        """Determines whether this map contains one or more keys for the
        specified value.

        Args:
            value: The specified value.

        Returns:
            ``True`` if this map contains an entry for the specified value,
            ``False`` otherwise.
        """
        check_not_none(value, "value can't be None")
        try:
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.contains_value, value)

        request = replicated_map_contains_value_codec.encode_request(self.name, value_data)
        return await self._ainvoke_on_partition(
            request, self._partition_id, replicated_map_contains_value_codec.decode_response
        )

    async def entry_set(self) -> typing.List[typing.Tuple[KeyType, ValueType]]:
        """Returns a List clone of the mappings contained in this map.

        Warning:
            The list is NOT backed by the map, so changes to the map are NOT
            reflected in the list, and vice-versa.

        Returns:
            The list of key-value tuples in the map.
        """

        def handler(message):
            entry_data_list = replicated_map_entry_set_codec.decode_response(message)
            return deserialize_entry_list_in_place(entry_data_list, self._to_object)

        request = replicated_map_entry_set_codec.encode_request(self.name)
        return await self._ainvoke_on_partition(request, self._partition_id, handler)

    async def get(self, key: KeyType) -> typing.Optional[ValueType]:
        """Returns the value for the specified key, or ``None`` if this map
        does not contain this key.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: The specified key.

        Returns:
            The value associated with the specified key.
        """
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.get, key)

        def handler(message):
            return self._to_object(replicated_map_get_codec.decode_response(message))

        request = replicated_map_get_codec.encode_request(self.name, key_data)
        return await self._invoke_on_key(request, key_data, handler)

    async def is_empty(self) -> bool:
        """Returns ``True`` if this map contains no key-value mappings.

        Returns:
            ``True`` if this map contains no key-value mappings.
        """
        request = replicated_map_is_empty_codec.encode_request(self.name)
        return await self._ainvoke_on_partition(
            request, self._partition_id, replicated_map_is_empty_codec.decode_response
        )

    async def key_set(self) -> typing.List[KeyType]:
        """Returns the list of keys in the ReplicatedMap.

        Warning:
            The list is NOT backed by the map, so changes to the map are NOT
            reflected in the list, and vice-versa.

        Returns:
            A list of the clone of the keys.
        """

        def handler(message):
            data_list = replicated_map_key_set_codec.decode_response(message)
            return deserialize_list_in_place(data_list, self._to_object)

        request = replicated_map_key_set_codec.encode_request(self.name)
        return await self._ainvoke_on_partition(request, self._partition_id, handler)

    async def put(
        self, key: KeyType, value: ValueType, ttl: float = 0
    ) -> typing.Optional[ValueType]:
        """Associates the specified value with the specified key in this map.

        If the map previously contained a mapping for the key, the old value
        is replaced by the specified value. If ttl is provided, entry will
        expire and get evicted after the ttl.

        Args:
            key: The specified key.
            value: The value to associate with the key.
            ttl: Maximum time in seconds for this entry to stay, if not
                provided, the value configured on server side configuration
                will be used.

        Returns:
            Previous value associated with key or ``None`` if there was no
            mapping for key.
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.put, key, value, ttl)

        def handler(message):
            return self._to_object(replicated_map_put_codec.decode_response(message))

        request = replicated_map_put_codec.encode_request(
            self.name, key_data, value_data, to_millis(ttl)
        )
        return await self._invoke_on_key(request, key_data, handler)

    async def put_all(self, source: typing.Dict[KeyType, ValueType]) -> None:
        """Copies all the mappings from the specified map to this map.

        No atomicity guarantees are given. In the case of a failure,
        some key-value tuples may get written, while others are not.

        Args:
            source: Map which includes mappings to be stored in this map.
        """
        try:
            entries = []
            for key, value in source.items():
                check_not_none(key, "key can't be None")
                check_not_none(value, "value can't be None")
                entries.append((self._to_data(key), self._to_data(value)))
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.put_all, source)

        request = replicated_map_put_all_codec.encode_request(self.name, entries)
        return await self._invoke(request)

    async def remove(self, key: KeyType) -> typing.Optional[ValueType]:
        """Removes the mapping for a key from this map if it is present.

        The map will not contain a mapping for the specified key once the call
        returns.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: Key of the mapping to be deleted.

        Returns:
            The previous value associated with key, or ``None`` if there was
            no mapping for key.
        """
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.remove, key)

        def handler(message):
            return self._to_object(replicated_map_remove_codec.decode_response(message))

        request = replicated_map_remove_codec.encode_request(self.name, key_data)
        return await self._invoke_on_key(request, key_data, handler)

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
        """Returns the number of entries in this replicated map.

        Returns:
            Number of entries in this replicated map.
        """
        request = replicated_map_size_codec.encode_request(self.name)
        return await self._ainvoke_on_partition(
            request, self._partition_id, replicated_map_size_codec.decode_response
        )

    async def values(self) -> typing.List[ValueType]:
        """Returns the list of values in the map.

        Warning:
            The returned list is NOT backed by the map, so changes to the map
            are NOT reflected in the list, and vice-versa.

        Returns:
            The list of values in the map.
        """

        def handler(message):
            data_list = replicated_map_values_codec.decode_response(message)
            return deserialize_list_in_place(data_list, self._to_object)

        request = replicated_map_values_codec.encode_request(self.name)
        return await self._ainvoke_on_partition(request, self._partition_id, handler)


async def create_replicated_map_proxy(service_name, name, context):
    return ReplicatedMap(service_name, name, context)
