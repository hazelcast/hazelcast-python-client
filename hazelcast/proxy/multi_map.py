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
from hazelcast.util import check_not_none, thread_id, to_millis, ImmutableLazyDataList


class MultiMap(Proxy):
    """A specialized map whose keys can be associated with multiple values."""

    def __init__(self, service_name, name, context):
        super(MultiMap, self).__init__(service_name, name, context)
        self._reference_id_generator = context.lock_reference_id_generator

    def add_entry_listener(
        self, include_value=False, key=None, added_func=None, removed_func=None, clear_all_func=None
    ):
        """Adds an entry listener for this multimap.

        The listener will be notified for all multimap add/remove/clear-all events.

        Args:
            include_value (bool): Whether received event should include the value or not.
            key: Key for filtering the events.
            added_func (function): Function to be called when an entry is added to map.
            removed_func (function): Function to be called when an entry is removed from map.
            clear_all_func (function): Function to be called when entries are cleared from map.

        Returns:
            hazelcast.future.Future[str]: A registration id which is used as a key to remove the listener.
        """
        if key:
            codec = multi_map_add_entry_listener_to_key_codec
            key_data = self._to_data(key)
            request = codec.encode_request(self.name, key_data, include_value, False)
        else:
            codec = multi_map_add_entry_listener_codec
            request = codec.encode_request(self.name, include_value, False)

        def handle_event_entry(
            key, value, old_value, merging_value, event_type, uuid, number_of_affected_entries
        ):
            event = EntryEvent(
                self._to_object,
                key,
                value,
                old_value,
                merging_value,
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
            lambda r: codec.decode_response(r),
            lambda reg_id: multi_map_remove_entry_listener_codec.encode_request(self.name, reg_id),
            lambda m: codec.handle(m, handle_event_entry),
        )

    def contains_key(self, key):
        """Determines whether this multimap contains an entry with the key.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form of the key, not the
            actual implementations of ``__hash__`` and ``__eq__`` defined in key's class.

        Args:
            key: The specified key.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if this multimap contains an entry for the specified key,
            ``False`` otherwise.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)

        request = multi_map_contains_key_codec.encode_request(self.name, key_data, thread_id())
        return self._invoke_on_key(request, key_data, multi_map_contains_key_codec.decode_response)

    def contains_value(self, value):
        """Determines whether this map contains one or more keys for the specified value.

        Args:
            value: The specified value.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if this multimap contains an entry for the specified value,
            ``False`` otherwise.
        """
        check_not_none(value, "value can't be None")
        value_data = self._to_data(value)
        request = multi_map_contains_value_codec.encode_request(self.name, value_data)
        return self._invoke(request, multi_map_contains_value_codec.decode_response)

    def contains_entry(self, key, value):
        """Returns whether the multimap contains an entry with the value.

        Args:
            key: The specified key.
            value: The specified value.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if this multimap contains the key-value tuple, ``False`` otherwise.
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        key_data = self._to_data(key)
        value_data = self._to_data(value)

        request = multi_map_contains_entry_codec.encode_request(
            self.name, key_data, value_data, thread_id()
        )
        return self._invoke_on_key(
            request, key_data, multi_map_contains_entry_codec.decode_response
        )

    def clear(self):
        """Clears the multimap. Removes all key-value tuples.

        Returns:
            hazelcast.future.Future[None]:
        """
        request = multi_map_clear_codec.encode_request(self.name)
        return self._invoke(request)

    def entry_set(self):
        """Returns the list of key-value tuples in the multimap.

        Warning:
            The list is NOT backed by the map, so changes to the map are NOT reflected in the list, and vice-versa.

        Returns:
            hazelcast.future.Future[list]: The list of key-value tuples in the multimap.
        """

        def handler(message):
            return ImmutableLazyDataList(
                multi_map_entry_set_codec.decode_response(message), self._to_object
            )

        request = multi_map_entry_set_codec.encode_request(self.name)
        return self._invoke(request, handler)

    def get(self, key):
        """Returns the list of values associated with the key. ``None`` if this map does not contain this key.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` of the binary form of the key, not the
            actual implementations of ``__hash__`` and ``__eq__`` defined in the key's class.

        Warning:
            The list is NOT backed by the multimap, so changes to the map are list reflected in the collection, and
            vice-versa.

        Args:
            key: The specified key.

        Returns:
            hazelcast.future.Future[list]: The list of the values associated with the specified key.
        """
        check_not_none(key, "key can't be None")

        def handler(message):
            return ImmutableLazyDataList(
                multi_map_get_codec.decode_response(message), self._to_object
            )

        key_data = self._to_data(key)
        request = multi_map_get_codec.encode_request(self.name, key_data, thread_id())
        return self._invoke_on_key(request, key_data, handler)

    def is_locked(self, key):
        """Checks the lock for the specified key.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form of the key, not the
            actual implementations of ``__hash__`` and ``__eq__`` defined in key's class.

        Args:
            key: The key that is checked for lock.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if lock is acquired, ``False`` otherwise.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)

        request = multi_map_is_locked_codec.encode_request(self.name, key_data)
        return self._invoke_on_key(request, key_data, multi_map_is_locked_codec.decode_response)

    def force_unlock(self, key):
        """Releases the lock for the specified key regardless of the lock owner.

        It always successfully unlocks the key, never blocks, and returns immediately.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form of the key, not the
            actual implementations of ``__hash__`` and ``__eq__`` defined in key's class.

        Args:
            key: The key to lock.

        Returns:
            hazelcast.future.Future[None]:
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        request = multi_map_force_unlock_codec.encode_request(
            self.name, key_data, self._reference_id_generator.get_and_increment()
        )
        return self._invoke_on_key(request, key_data)

    def key_set(self):
        """Returns the list of keys in the multimap.

        Warning:
            The list is NOT backed by the map, so changes to the map are NOT reflected in the list, and vice-versa.

        Returns:
            hazelcast.future.Future[list]: A list of the clone of the keys.
        """

        def handler(message):
            return ImmutableLazyDataList(
                multi_map_key_set_codec.decode_response(message), self._to_object
            )

        request = multi_map_key_set_codec.encode_request(self.name)
        return self._invoke(request, handler)

    def lock(self, key, lease_time=None):
        """Acquires the lock for the specified key infinitely or for the specified lease time if provided.

        If the lock is not available, the current thread becomes disabled for thread scheduling purposes and lies
        dormant until the lock has been acquired.

        Scope of the lock is this map only. Acquired lock is only for the key in this map.

        Locks are re-entrant; so, if the key is locked N times, it should be unlocked N times before another thread can
        acquire it.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form of the key, not the
            actual implementations of ``__hash__`` and ``__eq__`` defined in key's class.

        Args:
            key: The key to lock.
            lease_time (int): Time in seconds to wait before releasing the lock.

        Returns:
            hazelcast.future.Future[None]:
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        request = multi_map_lock_codec.encode_request(
            self.name,
            key_data,
            thread_id(),
            to_millis(lease_time),
            self._reference_id_generator.get_and_increment(),
        )
        return self._invoke_on_key(request, key_data)

    def remove(self, key, value):
        """Removes the given key-value tuple from the multimap.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form of the key, not the
            actual implementations of ``__hash__`` and ``__eq__`` defined in key's class.

        Args:
            key: The key of the entry to remove.
            value: The value of the entry to remove.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if the size of the multimap changed after the remove operation,
            ``False`` otherwise.
        """
        check_not_none(key, "key can't be None")
        check_not_none(key, "value can't be None")
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        request = multi_map_remove_entry_codec.encode_request(
            self.name, key_data, value_data, thread_id()
        )
        return self._invoke_on_key(request, key_data, multi_map_remove_entry_codec.decode_response)

    def remove_all(self, key):
        """Removes all the entries with the given key and returns the value list associated with this key.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form of the key, not the
            actual implementations of ``__hash__`` and ``__eq__`` defined in key's class.

        Warning:
            The returned list is NOT backed by the map, so changes to the map are NOT reflected in the list, and
            vice-versa.

        Args:
            key: The key of the entries to remove.

        Returns:
            hazelcast.future.Future[list]: The collection of removed values associated with the given key.
        """
        check_not_none(key, "key can't be None")

        def handler(message):
            return ImmutableLazyDataList(
                multi_map_remove_codec.decode_response(message), self._to_object
            )

        key_data = self._to_data(key)
        request = multi_map_remove_codec.encode_request(self.name, key_data, thread_id())
        return self._invoke_on_key(request, key_data, handler)

    def put(self, key, value):
        """Stores a key-value tuple in the multimap.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form of the key, not the
            actual implementations of ``__hash__`` and ``__eq__`` defined in key's class.

        Args:
            key: The key to be stored.
            value: The value to be stored.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if size of the multimap is increased,
            ``False`` if the multimap already contains the key-value tuple.
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        request = multi_map_put_codec.encode_request(self.name, key_data, value_data, thread_id())
        return self._invoke_on_key(request, key_data, multi_map_put_codec.decode_response)

    def remove_entry_listener(self, registration_id):
        """Removes the specified entry listener.

        Returns silently if there is no such listener added before.

        Args:
            registration_id (str): Id of registered listener.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if registration is removed, ``False`` otherwise.
        """
        return self._deregister_listener(registration_id)

    def size(self):
        """Returns the number of entries in this multimap.

        Returns:
            hazelcast.future.Future[int]: Number of entries in this multimap.
        """
        request = multi_map_size_codec.encode_request(self.name)
        return self._invoke(request, multi_map_size_codec.decode_response)

    def value_count(self, key):
        """Returns the number of values that match the given key in the multimap.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form of the key, not the
            actual implementations of ``__hash__`` and ``__eq__`` defined in key's class.

        Args:
            key: The key whose values count is to be returned.

        Returns:
            hazelcast.future.Future[int]: The number of values that match the given key in the multimap.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        request = multi_map_value_count_codec.encode_request(self.name, key_data, thread_id())
        return self._invoke_on_key(request, key_data, multi_map_value_count_codec.decode_response)

    def values(self):
        """Returns the list of values in the multimap.

        Warning:
            The returned list is NOT backed by the map, so changes to the map are NOT reflected in the list, and
            vice-versa.

        Returns:
            hazelcast.future.Future[list]: The list of values in the multimap.
        """

        def handler(message):
            return ImmutableLazyDataList(
                multi_map_values_codec.decode_response(message), self._to_object
            )

        request = multi_map_values_codec.encode_request(self.name)
        return self._invoke(request, handler)

    def try_lock(self, key, lease_time=None, timeout=0):
        """Tries to acquire the lock for the specified key.

        When the lock is not available:

        - If the timeout is not provided, the current thread doesn't wait and returns ``False`` immediately.
        - If the timeout is provided, the current thread becomes disabled for thread scheduling purposes and lies
          dormant until one of the followings happens:

            - The lock is acquired by the current thread, or
            - The specified waiting time elapses.

        If the lease time is provided, lock will be released after this time elapses.

        Args:
            key: Key to lock in this map.
            lease_time (int): Time in seconds to wait before releasing the lock.
            timeout (int): Maximum time in seconds to wait for the lock.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if the lock was acquired, ``False`` otherwise.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        request = multi_map_try_lock_codec.encode_request(
            self.name,
            key_data,
            thread_id(),
            to_millis(lease_time),
            to_millis(timeout),
            self._reference_id_generator.get_and_increment(),
        )
        return self._invoke_on_key(request, key_data, multi_map_try_lock_codec.decode_response)

    def unlock(self, key):
        """Releases the lock for the specified key. It never blocks and returns immediately.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form of the key, not the
            actual implementations of ``__hash__`` and ``__eq__`` defined in key's class.

        Args:
            key: The key to lock.

        Returns:
            hazelcast.future.Future[None]:
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        request = multi_map_unlock_codec.encode_request(
            self.name, key_data, thread_id(), self._reference_id_generator.get_and_increment()
        )
        return self._invoke_on_key(request, key_data)
