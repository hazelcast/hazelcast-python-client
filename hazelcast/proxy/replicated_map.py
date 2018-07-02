from random import randint
from hazelcast.protocol.codec import replicated_map_clear_codec, replicated_map_add_entry_listener_codec, \
    replicated_map_add_entry_listener_to_key_codec, replicated_map_add_entry_listener_to_key_with_predicate_codec, \
    replicated_map_add_entry_listener_with_predicate_codec, replicated_map_contains_key_codec, \
    replicated_map_contains_value_codec, replicated_map_entry_set_codec, replicated_map_get_codec, \
    replicated_map_is_empty_codec, replicated_map_key_set_codec, replicated_map_put_all_codec, replicated_map_put_codec, \
    replicated_map_remove_codec, replicated_map_remove_entry_listener_codec, replicated_map_size_codec, \
    replicated_map_values_codec
from hazelcast.proxy.base import Proxy, default_response_handler, EntryEvent, EntryEventType
from hazelcast.util import to_millis, check_not_none
from hazelcast import six


class ReplicatedMap(Proxy):
    """
    A ReplicatedMap is a map-like data structure with weak consistency and values locally stored on every node of the
    cluster.

    Whenever a value is written asynchronously, the new value will be internally distributed to all existing cluster
    members, and eventually every node will have the new value.

    When a new node joins the cluster, the new node initially will request existing values from older nodes and
    replicate them locally.
    """
    _partition_id = None

    def add_entry_listener(self, key=None, predicate=None, added_func=None, removed_func=None, updated_func=None,
                           evicted_func=None, clear_all_func=None):
        """
        Adds a continuous entry listener for this map. Listener will get notified for map events filtered with given
        parameters.

        :param key: (object), key for filtering the events (optional).
        :param predicate: (Predicate), predicate for filtering the events (optional).
        :param added_func: Function to be called when an entry is added to map (optional).
        :param removed_func: Function to be called when an entry is removed from map (optional).
        :param updated_func: Function to be called when an entry is updated (optional).
        :param evicted_func: Function to be called when an entry is evicted from map (optional).
        :param clear_all_func: Function to be called when entries are cleared from map (optional).
        :return: (str), a registration id which is used as a key to remove the listener.

        .. seealso:: :class:`~hazelcast.serialization.predicate.Predicate` for more info about predicates.
        """
        if key and predicate:
            key_data = self._to_data(key)
            predicate_data = self._to_data(predicate)
            request = replicated_map_add_entry_listener_to_key_with_predicate_codec.encode_request(self.name, key_data,
                                                                                                   predicate_data,
                                                                                                   False)
        elif key and not predicate:
            key_data = self._to_data(key)
            request = replicated_map_add_entry_listener_to_key_codec.encode_request(self.name, key_data, False)
        elif not key and predicate:
            predicate = self._to_data(predicate)
            request = replicated_map_add_entry_listener_with_predicate_codec.encode_request(self.name, predicate,
                                                                                            False)
        else:
            request = replicated_map_add_entry_listener_codec.encode_request(self.name, False)

        def handle_event_entry(**_kwargs):
            event = EntryEvent(self._to_object, **_kwargs)
            if event.event_type == EntryEventType.added and added_func:
                added_func(event)
            elif event.event_type == EntryEventType.removed and removed_func:
                removed_func(event)
            elif event.event_type == EntryEventType.updated and updated_func:
                updated_func(event)
            elif event.event_type == EntryEventType.evicted and evicted_func:
                evicted_func(event)
            elif event.event_type == EntryEventType.clear_all and clear_all_func:
                clear_all_func(event)

        return self._start_listening(request,
                                     lambda m: replicated_map_add_entry_listener_codec.handle(m,
                                                                                              handle_event_entry),
                                     lambda r: replicated_map_add_entry_listener_codec.decode_response(r)[
                                         'response'])

    def clear(self):
        """
        Wipes data out of the replicated map.
        """
        return self._encode_invoke(replicated_map_clear_codec)

    def contains_key(self, key):
        """
        Determines whether this map contains an entry with the key.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), the specified key.
        :return: (bool), ``true`` if this map contains an entry for the specified key.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(replicated_map_contains_key_codec, key_data, key=key_data)

    def contains_value(self, value):
        """
        Determines whether this map contains one or more keys for the specified value.

        :param value: (object), the specified value.
        :return: (bool), ``true`` if this map contains an entry for the specified value.
        """
        check_not_none(value, "value can't be None")
        return self._encode_invoke_on_target_partition(replicated_map_contains_value_codec, value=self._to_data(value))

    def entry_set(self):
        """
        Returns a List clone of the mappings contained in this map.

        **Warning:
        The list is NOT backed by the map, so changes to the map are NOT reflected in the list, and vice-versa.**

        :return: (Sequence), the list of key-value tuples in the map.
        """
        return self._encode_invoke_on_target_partition(replicated_map_entry_set_codec)

    def get(self, key):
        """
        Returns the value for the specified key, or None if this map does not contain this key.

        **Warning:
        This method uses hashCode and equals of the binary form of the key, not the actual implementations of hashCode
        and equals defined in the key's class.**

        :param key: (object), the specified key.
        :return: (Sequence), the list of the values associated with the specified key.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(replicated_map_get_codec, key_data, key=key_data)

    def is_empty(self):
        """
        Returns ``true`` if this map contains no key-value mappings.

        :return: (bool), ``true`` if this map contains no key-value mappings.
        """
        return self._encode_invoke_on_target_partition(replicated_map_is_empty_codec)

    def key_set(self):
        """
        Returns the list of keys in the ReplicatedMap.

        **Warning:
        The list is NOT backed by the map, so changes to the map are NOT reflected in the list, and vice-versa.**

        :return: (Sequence), a list of the clone of the keys.
        """
        return self._encode_invoke_on_target_partition(replicated_map_key_set_codec)

    def put(self, key, value, ttl=0):
        """
        Associates the specified value with the specified key in this map. If the map previously contained a mapping for
        the key, the old value is replaced by the specified value. If ttl is provided, entry will expire and get evicted
        after the ttl.

        :param key: (object), the specified key.
        :param value: (object), the value to associate with the key.
        :param ttl: (int), maximum time in seconds for this entry to stay, if not provided, the value configured on
            server side configuration will be used(optional).
        :return: (object), previous value associated with key or None if there was no mapping for key.
        """
        check_not_none(key, "key can't be None")
        check_not_none(key, "value can't be None")
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._encode_invoke_on_key(replicated_map_put_codec, key_data, key=key_data, value=value_data,
                                          ttl=to_millis(ttl))

    def put_all(self, map):
        """
        Copies all of the mappings from the specified map to this map. No atomicity guarantees are
        given. In the case of a failure, some of the key-value tuples may get written, while others are not.

        :param map: (dict), map which includes mappings to be stored in this map.
        """
        entries = {}
        for key, value in six.iteritems(map):
            check_not_none(key, "key can't be None")
            check_not_none(value, "value can't be None")
            entries[self._to_data(key)] = self._to_data(value)
        self._encode_invoke(replicated_map_put_all_codec, entries=entries)

    def remove(self, key):
        """
        Removes the mapping for a key from this map if it is present. The map will not contain a mapping for the
        specified key once the call returns.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), key of the mapping to be deleted.
        :return: (object), the previous value associated with key, or None if there was no mapping for key.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(replicated_map_remove_codec, key_data, key=key_data)

    def remove_entry_listener(self, registration_id):
        """
        Removes the specified entry listener. Returns silently if there is no such listener added before.

        :param registration_id: (str), id of registered listener.
        :return: (bool), ``true`` if registration is removed, ``false`` otherwise.
        """
        return self._stop_listening(registration_id,
                                    lambda i: replicated_map_remove_entry_listener_codec.encode_request(self.name, i))

    def size(self):
        """
        Returns the number of entries in this multimap.

        :return: (int), number of entries in this multimap.
        """
        return self._encode_invoke_on_target_partition(replicated_map_size_codec)

    def values(self):
        """
        Returns the list of values in the map.

        **Warning:
        The returned list is NOT backed by the map, so changes to the map are NOT reflected in the list, and
        vice-versa.**

        :return: (Sequence), the list of values in the map.
        """
        return self._encode_invoke_on_target_partition(replicated_map_values_codec)

    def _get_partition_id(self):
        if not self._partition_id:
            self._partition_id = randint(0, self._client.partition_service.get_partition_count() - 1)
        return self._partition_id

    def _encode_invoke_on_target_partition(self, codec, response_handler=default_response_handler, **kwargs):
        return self._encode_invoke_on_partition(codec, self._get_partition_id(), response_handler, **kwargs)
