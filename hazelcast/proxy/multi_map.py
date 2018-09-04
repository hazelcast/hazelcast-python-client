from hazelcast.protocol.codec import multi_map_add_entry_listener_codec, multi_map_add_entry_listener_to_key_codec, \
    multi_map_clear_codec, multi_map_contains_entry_codec, multi_map_contains_key_codec, multi_map_contains_value_codec, \
    multi_map_entry_set_codec, multi_map_force_unlock_codec, multi_map_get_codec, multi_map_is_locked_codec, \
    multi_map_key_set_codec, multi_map_lock_codec, multi_map_put_codec, multi_map_remove_codec, \
    multi_map_remove_entry_codec, multi_map_remove_entry_listener_codec, multi_map_size_codec, multi_map_try_lock_codec, \
    multi_map_unlock_codec, multi_map_value_count_codec, multi_map_values_codec
from hazelcast.proxy.base import Proxy, EntryEvent, EntryEventType
from hazelcast.util import check_not_none, thread_id, to_millis


class MultiMap(Proxy):
    """
    A specialized map whose keys can be associated with multiple values.
    """
    def __init__(self, client, service_name, name):
        super(MultiMap, self).__init__(client, service_name, name)
        self.reference_id_generator = self._client.lock_reference_id_generator

    def add_entry_listener(self, include_value=False, key=None, added_func=None, removed_func=None, clear_all_func=None):
        """
        Adds an entry listener for this multimap. The listener will be notified for all multimap add/remove/clear-all
        events.

        :param include_value: (bool), whether received events include an old value or not (optional).
        :param key: (object), key for filtering the events (optional).
        :param added_func: Function to be called when an entry is added to map (optional).
        :param removed_func: Function to be called when an entry is removed_func from map (optional).
        :param clear_all_func: Function to be called when entries are cleared from map (optional).
        :return: (str), a registration id which is used as a key to remove the listener.
        """
        if key:
            key_data = self._to_data(key)
            request = multi_map_add_entry_listener_to_key_codec.encode_request(name=self.name, key=key_data,
                                                                               include_value=include_value,
                                                                               local_only=False)
        else:
            request = multi_map_add_entry_listener_codec.encode_request(name=self.name, include_value=include_value,
                                                                        local_only=False)

        def handle_event_entry(**_kwargs):
            event = EntryEvent(self._to_object, **_kwargs)
            if event.event_type == EntryEventType.added and added_func:
                added_func(event)
            elif event.event_type == EntryEventType.removed and removed_func:
                removed_func(event)
            elif event.event_type == EntryEventType.clear_all and clear_all_func:
                clear_all_func(event)

        return self._start_listening(request,
                                     lambda m: multi_map_add_entry_listener_codec.handle(m,
                                                                                         handle_event_entry),
                                     lambda r: multi_map_add_entry_listener_codec.decode_response(r)[
                                         'response'])

    def contains_key(self, key):
        """
        Determines whether this multimap contains an entry with the key.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), the specified key.
        :return: (bool), ``true`` if this multimap contains an entry for the specified key.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_contains_key_codec, key_data, key=key_data,
                                          thread_id=thread_id())

    def contains_value(self, value):
        """
        Determines whether this map contains one or more keys for the specified value.

        :param value: (object), the specified value.
        :return: (bool), ``true`` if this multimap contains an entry for the specified value.
        """
        check_not_none(value, "value can't be None")
        value_data = self._to_data(value)
        return self._encode_invoke(multi_map_contains_value_codec, value=value_data)

    def contains_entry(self, key, value):
        """
        Returns whether the multimap contains an entry with the value.

        :param key: (object), the specified key.
        :param value: (object), the specified value.
        :return: (bool), ``true`` if this multimap contains the key-value tuple.
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._encode_invoke_on_key(multi_map_contains_entry_codec, key_data, key=key_data,
                                          value=value_data, thread_id=thread_id())

    def clear(self):
        """
        Clears the multimap. Removes all key-value tuples.
        """
        return self._encode_invoke(multi_map_clear_codec)

    def entry_set(self):
        """
        Returns the list of key-value tuples in the multimap.

        **Warning:
        The list is NOT backed by the map, so changes to the map are NOT reflected in the list, and vice-versa.**

        :return: (Sequence), the list of key-value tuples in the multimap.
        """
        return self._encode_invoke(multi_map_entry_set_codec)

    def get(self, key):
        """
        Returns the list of values associated with the key. ``None`` if this map does not contain this key.

        **Warning:
        This method uses hashCode and equals of the binary form of the key, not the actual implementations of hashCode
        and equals defined in the key's class.**

        **Warning-2:
        The list is NOT backed by the multimap, so changes to the map are list reflected in the collection, and
        vice-versa.**

        :param key: (object), the specified key.
        :return: (Sequence), the list of the values associated with the specified key.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_get_codec, key_data, key=key_data,
                                          thread_id=thread_id())

    def is_locked(self, key):
        """
        Checks the lock for the specified key. If the lock is acquired, returns ``true``. Otherwise, returns false.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), the key that is checked for lock.
        :return: (bool), ``true`` if lock is acquired, false otherwise.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_is_locked_codec, key_data, key=key_data)

    def force_unlock(self, key):
        """
        Releases the lock for the specified key regardless of the lock owner. It always successfully unlocks the key,
        never blocks, and returns immediately.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), the key to lock.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_force_unlock_codec, key_data, key=key_data,
                                          reference_id=self.reference_id_generator.get_next())

    def key_set(self):
        """
        Returns the list of keys in the multimap.

        **Warning:
        The list is NOT backed by the map, so changes to the map are NOT reflected in the list, and vice-versa.**

        :return: (Sequence), a list of the clone of the keys.
        """
        return self._encode_invoke(multi_map_key_set_codec)

    def lock(self, key, lease_time=-1):
        """
        Acquires the lock for the specified key infinitely or for the specified lease time if provided.

        If the lock is not available, the current thread becomes disabled for thread scheduling purposes and lies
        dormant until the lock has been acquired.

        Scope of the lock is this map only. Acquired lock is only for the key in this map.

        Locks are re-entrant; so, if the key is locked N times, it should be unlocked N times before another thread can
        acquire it.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), the key to lock.
        :param lease_time: (int), time in seconds to wait before releasing the lock (optional).
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_lock_codec, key_data, key=key_data,
                                          thread_id=thread_id(), ttl=to_millis(lease_time),
                                          reference_id=self.reference_id_generator.get_next())

    def remove(self, key, value):
        """
        Removes the given key-value tuple from the multimap.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object),  the key of the entry to remove.
        :param value: (object), the value of the entry to remove.
        :return: (bool), ``true`` if the size of the multimap changed after the remove operation, ``false`` otherwise.
        """
        check_not_none(key, "key can't be None")
        check_not_none(key, "value can't be None")
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._encode_invoke_on_key(multi_map_remove_entry_codec, key_data, key=key_data,
                                          value=value_data, thread_id=thread_id())

    def remove_all(self, key):
        """
        Removes all the entries with the given key and returns the value list associated with this key.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        **Warning-2:
        The returned list is NOT backed by the map, so changes to the map are NOT reflected in the list, and
        vice-versa.**

        :param key: (object), the key of the entries to remove.
        :return: (Sequence), the collection of removed values associated with the given key.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_remove_codec, key_data, key=key_data,
                                          thread_id=thread_id())

    def put(self, key, value):
        """
        Stores a key-value tuple in the multimap.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), the key to be stored.
        :param value: (object), the value to be stored.
        :return: (bool), ``true`` if size of the multimap is increased, ``false`` if the multimap already contains the key-value
        tuple.
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._encode_invoke_on_key(multi_map_put_codec, key_data, key=key_data, value=value_data,
                                          thread_id=thread_id())

    def remove_entry_listener(self, registration_id):
        """
        Removes the specified entry listener. Returns silently if there is no such listener added before.

        :param registration_id: (str), id of registered listener.
        :return: (bool), ``true`` if registration is removed, ``false`` otherwise.
        """
        return self._stop_listening(registration_id,
                                    lambda i: multi_map_remove_entry_listener_codec.encode_request(self.name, i))

    def size(self):
        """
        Returns the number of entries in this multimap.

        :return: (int), number of entries in this multimap.
        """
        return self._encode_invoke(multi_map_size_codec)

    def value_count(self, key):
        """
        Returns the number of values that match the given key in the multimap.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), the key whose values count is to be returned.
        :return: (int), the number of values that match the given key in the multimap.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_value_count_codec, key_data, key=key_data,
                                          thread_id=thread_id())

    def values(self):
        """
        Returns the list of values in the multimap.

        **Warning:
        The returned list is NOT backed by the map, so changes to the map are NOT reflected in the list, and
        vice-versa.**

        :return: (Sequence), the list of values in the multimap.
        """
        return self._encode_invoke(multi_map_values_codec)

    def try_lock(self, key, lease_time=-1, timeout=-1):
        """
        Tries to acquire the lock for the specified key. When the lock is not available,

            * If timeout is not provided, the current thread doesn't wait and returns ``false`` immediately.
            * If a timeout is provided, the current thread becomes disabled for thread scheduling purposes and lies
              dormant until one of the followings happens:
                * the lock is acquired by the current thread, or
                * the specified waiting time elapses.

        If lease_time is provided, lock will be released after this time elapses.

        :param key: (object), key to lock in this map.
        :param lease_time: (int), time in seconds to wait before releasing the lock (optional).
        :param timeout: (int), maximum time in seconds to wait for the lock (optional).
        :return: (bool), ``true`` if the lock was acquired and otherwise, false.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_try_lock_codec, key_data, key=key_data,
                                          thread_id=thread_id(), lease=to_millis(lease_time),
                                          timeout=to_millis(timeout),
                                          reference_id=self.reference_id_generator.get_next())

    def unlock(self, key):
        """
        Releases the lock for the specified key. It never blocks and returns immediately.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), the key to lock.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_unlock_codec, key_data, key=key_data,
                                          thread_id=thread_id(), reference_id=self.reference_id_generator.get_next())
