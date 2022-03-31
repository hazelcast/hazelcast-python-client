import typing

from hazelcast.future import Future
from hazelcast.protocol.codec import (
    multi_map_add_entry_listener_codec,
    multi_map_add_entry_listener_to_key_codec,
    multi_map_clear_codec,
    multi_map_contains_entry_codec,
    multi_map_contains_key_codec,
    multi_map_contains_value_codec,
    multi_map_entry_set_codec,
    multi_map_force_unlock_codec,
    multi_map_get_codec,
    multi_map_is_locked_codec,
    multi_map_key_set_codec,
    multi_map_lock_codec,
    multi_map_put_codec,
    multi_map_remove_codec,
    multi_map_remove_entry_codec,
    multi_map_remove_entry_listener_codec,
    multi_map_size_codec,
    multi_map_try_lock_codec,
    multi_map_unlock_codec,
    multi_map_value_count_codec,
    multi_map_values_codec,
)
from hazelcast.proxy.base import Proxy, EntryEvent, EntryEventType
from hazelcast.types import ValueType, KeyType
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import check_not_none, thread_id, to_millis, ImmutableLazyDataList


EntryEventCallable = typing.Callable[[EntryEvent[KeyType, ValueType]], None]


class MultiMap(Proxy["BlockingMultiMap"], typing.Generic[KeyType, ValueType]):
    """A specialized map whose keys can be associated with multiple values."""

    def __init__(self, service_name, name, context):
        super(MultiMap, self).__init__(service_name, name, context)
        self._reference_id_generator = context.lock_reference_id_generator

    def add_entry_listener(
        self,
        include_value: bool = False,
        key: KeyType = None,
        added_func: EntryEventCallable = None,
        removed_func: EntryEventCallable = None,
        clear_all_func: EntryEventCallable = None,
    ) -> Future[str]:
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
                return self._send_schema_and_retry(
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
            if event.event_type == EntryEventType.ADDED and added_func:
                added_func(event)
            elif event.event_type == EntryEventType.REMOVED and removed_func:
                removed_func(event)
            elif event.event_type == EntryEventType.CLEAR_ALL and clear_all_func:
                clear_all_func(event)

        return self._register_listener(
            request,
            lambda r: response_decoder(r),
            lambda reg_id: multi_map_remove_entry_listener_codec.encode_request(self.name, reg_id),
            lambda m: event_message_handler(m, handle_event_entry),
        )

    def contains_key(self, key: KeyType) -> Future[bool]:
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
            return self._send_schema_and_retry(e, self.contains_key, key)

        request = multi_map_contains_key_codec.encode_request(self.name, key_data, thread_id())
        return self._invoke_on_key(request, key_data, multi_map_contains_key_codec.decode_response)

    def contains_value(self, value: ValueType) -> Future[bool]:
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
            return self._send_schema_and_retry(e, self.contains_value, value)

        request = multi_map_contains_value_codec.encode_request(self.name, value_data)
        return self._invoke(request, multi_map_contains_value_codec.decode_response)

    def contains_entry(self, key: KeyType, value: ValueType) -> Future[bool]:
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
            return self._send_schema_and_retry(e, self.contains_entry, key, value)

        request = multi_map_contains_entry_codec.encode_request(
            self.name, key_data, value_data, thread_id()
        )
        return self._invoke_on_key(
            request, key_data, multi_map_contains_entry_codec.decode_response
        )

    def clear(self) -> Future[None]:
        """Clears the multimap. Removes all key-value tuples."""
        request = multi_map_clear_codec.encode_request(self.name)
        return self._invoke(request)

    def entry_set(self) -> Future[typing.List[typing.Tuple[KeyType, ValueType]]]:
        """Returns the list of key-value tuples in the multimap.

        Warning:
            The list is NOT backed by the map, so changes to the map are NOT
            reflected in the list, and vice-versa.

        Returns:
            The list of key-value tuples in the multimap.
        """

        def handler(message):
            return ImmutableLazyDataList(
                multi_map_entry_set_codec.decode_response(message), self._to_object
            )

        request = multi_map_entry_set_codec.encode_request(self.name)
        return self._invoke(request, handler)

    def get(self, key: KeyType) -> Future[typing.Optional[typing.List[ValueType]]]:
        """Returns the list of values associated with the key. ``None`` if
        this map does not contain this key.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` of the binary form of
            the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in the key's class.

        Warning:
            The list is NOT backed by the multimap, so changes to the map are
            list reflected in the collection, and vice-versa.

        Args:
            key: The specified key.

        Returns:
            The list of the values associated with the specified key.
        """
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.get, key)

        def handler(message):
            return ImmutableLazyDataList(
                multi_map_get_codec.decode_response(message), self._to_object
            )

        request = multi_map_get_codec.encode_request(self.name, key_data, thread_id())
        return self._invoke_on_key(request, key_data, handler)

    def is_locked(self, key: KeyType) -> Future[bool]:
        """Checks the lock for the specified key.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: The key that is checked for lock.

        Returns:
            ``True`` if lock is acquired, ``False`` otherwise.
        """
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.is_locked, key)

        request = multi_map_is_locked_codec.encode_request(self.name, key_data)
        return self._invoke_on_key(request, key_data, multi_map_is_locked_codec.decode_response)

    def force_unlock(self, key: KeyType) -> Future[None]:
        """Releases the lock for the specified key regardless of the lock
        owner.

        It always successfully unlocks the key, never blocks, and returns
        immediately.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: The key to lock.
        """
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.force_unlock, key)

        request = multi_map_force_unlock_codec.encode_request(
            self.name, key_data, self._reference_id_generator.get_and_increment()
        )
        return self._invoke_on_key(request, key_data)

    def key_set(self) -> Future[typing.List[KeyType]]:
        """Returns the list of keys in the multimap.

        Warning:
            The list is NOT backed by the map, so changes to the map are NOT
            reflected in the list, and vice-versa.

        Returns:
            A list of the clone of the keys.
        """

        def handler(message):
            return ImmutableLazyDataList(
                multi_map_key_set_codec.decode_response(message), self._to_object
            )

        request = multi_map_key_set_codec.encode_request(self.name)
        return self._invoke(request, handler)

    def lock(self, key: KeyType, lease_time: float = None) -> Future[None]:
        """Acquires the lock for the specified key infinitely or for the
        specified lease time if provided.

        If the lock is not available, the current thread becomes disabled for
        thread scheduling purposes and lies dormant until the lock has been
        acquired.

        Scope of the lock is this map only. Acquired lock is only for the key
        in this map.

        Locks are re-entrant; so, if the key is locked N times, it should be
        unlocked N times before another thread can acquire it.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: The key to lock.
            lease_time: Time in seconds to wait before releasing the lock.
        """
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.lock, key, lease_time)

        request = multi_map_lock_codec.encode_request(
            self.name,
            key_data,
            thread_id(),
            to_millis(lease_time),
            self._reference_id_generator.get_and_increment(),
        )
        return self._invoke_on_key(request, key_data)

    def remove(self, key: KeyType, value: ValueType) -> Future[bool]:
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
        check_not_none(key, "value can't be None")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.remove, key, value)

        request = multi_map_remove_entry_codec.encode_request(
            self.name, key_data, value_data, thread_id()
        )
        return self._invoke_on_key(request, key_data, multi_map_remove_entry_codec.decode_response)

    def remove_all(self, key: KeyType) -> Future[typing.List[ValueType]]:
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
            return ImmutableLazyDataList(
                multi_map_remove_codec.decode_response(message), self._to_object
            )

        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.remove_all, key)

        request = multi_map_remove_codec.encode_request(self.name, key_data, thread_id())
        return self._invoke_on_key(request, key_data, handler)

    def put(self, key: KeyType, value: ValueType) -> Future[bool]:
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
            return self._send_schema_and_retry(e, self.put, key, value)

        request = multi_map_put_codec.encode_request(self.name, key_data, value_data, thread_id())
        return self._invoke_on_key(request, key_data, multi_map_put_codec.decode_response)

    def remove_entry_listener(self, registration_id: str) -> Future[bool]:
        """Removes the specified entry listener.

        Returns silently if there is no such listener added before.

        Args:
            registration_id: Id of registered listener.

        Returns:
            ``True`` if registration is removed, ``False`` otherwise.
        """
        return self._deregister_listener(registration_id)

    def size(self) -> Future[int]:
        """Returns the number of entries in this multimap.

        Returns:
            Number of entries in this multimap.
        """
        request = multi_map_size_codec.encode_request(self.name)
        return self._invoke(request, multi_map_size_codec.decode_response)

    def value_count(self, key: KeyType) -> Future[int]:
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
            return self._send_schema_and_retry(e, self.value_count, key)

        request = multi_map_value_count_codec.encode_request(self.name, key_data, thread_id())
        return self._invoke_on_key(request, key_data, multi_map_value_count_codec.decode_response)

    def values(self) -> Future[typing.List[ValueType]]:
        """Returns the list of values in the multimap.

        Warning:
            The returned list is NOT backed by the map, so changes to the map
            are NOT reflected in the list, and vice-versa.

        Returns:
            The list of values in the multimap.
        """

        def handler(message):
            return ImmutableLazyDataList(
                multi_map_values_codec.decode_response(message), self._to_object
            )

        request = multi_map_values_codec.encode_request(self.name)
        return self._invoke(request, handler)

    def try_lock(self, key: KeyType, lease_time: float = None, timeout: float = 0) -> Future[bool]:
        """Tries to acquire the lock for the specified key.

        When the lock is not available:

        - If the timeout is not provided, the current thread doesn't wait and
          returns ``False`` immediately.
        - If the timeout is provided, the current thread becomes disabled for
          thread scheduling purposes and lies dormant until one of the
          followings happens:

            - The lock is acquired by the current thread, or
            - The specified waiting time elapses.

        If the lease time is provided, lock will be released after this time
        elapses.

        Args:
            key: Key to lock in this map.
            lease_time: Time in seconds to wait before releasing the lock.
            timeout: Maximum time in seconds to wait for the lock.

        Returns:
            ``True`` if the lock was acquired, ``False`` otherwise.
        """
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.try_lock, key, lease_time, timeout)

        request = multi_map_try_lock_codec.encode_request(
            self.name,
            key_data,
            thread_id(),
            to_millis(lease_time),
            to_millis(timeout),
            self._reference_id_generator.get_and_increment(),
        )
        return self._invoke_on_key(request, key_data, multi_map_try_lock_codec.decode_response)

    def unlock(self, key: KeyType) -> Future[None]:
        """Releases the lock for the specified key. It never blocks and
        returns immediately.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: The key to lock.
        """
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.unlock, key)

        request = multi_map_unlock_codec.encode_request(
            self.name, key_data, thread_id(), self._reference_id_generator.get_and_increment()
        )
        return self._invoke_on_key(request, key_data)

    def blocking(self) -> "BlockingMultiMap[KeyType, ValueType]":
        return BlockingMultiMap(self)


class BlockingMultiMap(MultiMap[KeyType, ValueType]):
    __slots__ = ("_wrapped", "name", "service_name")

    def __init__(self, wrapped: MultiMap[KeyType, ValueType]):
        self.name = wrapped.name
        self.service_name = wrapped.service_name
        self._wrapped = wrapped

    def add_entry_listener(  # type: ignore[override]
        self,
        include_value: bool = False,
        key: KeyType = None,
        added_func: EntryEventCallable = None,
        removed_func: EntryEventCallable = None,
        clear_all_func: EntryEventCallable = None,
    ) -> str:
        return self._wrapped.add_entry_listener(
            include_value, key, added_func, removed_func, clear_all_func
        ).result()

    def contains_key(  # type: ignore[override]
        self,
        key: KeyType,
    ) -> bool:
        return self._wrapped.contains_key(key).result()

    def contains_value(  # type: ignore[override]
        self,
        value: ValueType,
    ) -> bool:
        return self._wrapped.contains_value(value).result()

    def contains_entry(  # type: ignore[override]
        self,
        key: KeyType,
        value: ValueType,
    ) -> bool:
        return self._wrapped.contains_entry(key, value).result()

    def clear(  # type: ignore[override]
        self,
    ) -> None:
        return self._wrapped.clear().result()

    def entry_set(  # type: ignore[override]
        self,
    ) -> typing.List[typing.Tuple[KeyType, ValueType]]:
        return self._wrapped.entry_set().result()

    def get(  # type: ignore[override]
        self,
        key: KeyType,
    ) -> typing.Optional[typing.List[ValueType]]:
        return self._wrapped.get(key).result()

    def is_locked(  # type: ignore[override]
        self,
        key: KeyType,
    ) -> bool:
        return self._wrapped.is_locked(key).result()

    def force_unlock(  # type: ignore[override]
        self,
        key: KeyType,
    ) -> None:
        return self._wrapped.force_unlock(key).result()

    def key_set(  # type: ignore[override]
        self,
    ) -> typing.List[KeyType]:
        return self._wrapped.key_set().result()

    def lock(  # type: ignore[override]
        self,
        key: KeyType,
        lease_time: float = None,
    ) -> None:
        return self._wrapped.lock(key, lease_time).result()

    def remove(  # type: ignore[override]
        self,
        key: KeyType,
        value: ValueType,
    ) -> bool:
        return self._wrapped.remove(key, value).result()

    def remove_all(  # type: ignore[override]
        self,
        key: KeyType,
    ) -> typing.List[ValueType]:
        return self._wrapped.remove_all(key).result()

    def put(  # type: ignore[override]
        self,
        key: KeyType,
        value: ValueType,
    ) -> bool:
        return self._wrapped.put(key, value).result()

    def remove_entry_listener(  # type: ignore[override]
        self,
        registration_id: str,
    ) -> bool:
        return self._wrapped.remove_entry_listener(registration_id).result()

    def size(  # type: ignore[override]
        self,
    ) -> int:
        return self._wrapped.size().result()

    def value_count(  # type: ignore[override]
        self,
        key: KeyType,
    ) -> int:
        return self._wrapped.value_count(key).result()

    def values(  # type: ignore[override]
        self,
    ) -> typing.List[ValueType]:
        return self._wrapped.values().result()

    def try_lock(  # type: ignore[override]
        self,
        key: KeyType,
        lease_time: float = None,
        timeout: float = 0,
    ) -> bool:
        return self._wrapped.try_lock(key, lease_time, timeout).result()

    def unlock(  # type: ignore[override]
        self,
        key: KeyType,
    ) -> None:
        return self._wrapped.unlock(key).result()

    def destroy(self) -> bool:
        return self._wrapped.destroy()

    def blocking(self) -> "BlockingMultiMap[KeyType, ValueType]":
        return self

    def __repr__(self) -> str:
        return self._wrapped.__repr__()
