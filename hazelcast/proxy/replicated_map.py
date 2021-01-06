from random import randint
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
from hazelcast.proxy.base import Proxy, EntryEvent, EntryEventType
from hazelcast.util import to_millis, check_not_none, ImmutableLazyDataList
from hazelcast import six


class ReplicatedMap(Proxy):
    """A ReplicatedMap is a map-like data structure with weak consistency and values locally stored on every node of the
    cluster.

    Whenever a value is written asynchronously, the new value will be internally distributed to all existing cluster
    members, and eventually every node will have the new value.

    When a new node joins the cluster, the new node initially will request existing values from older nodes and
    replicate them locally.
    """

    def __init__(self, service_name, name, context):
        super(ReplicatedMap, self).__init__(service_name, name, context)
        partition_service = context.partition_service
        self._partition_id = randint(0, partition_service.partition_count - 1)

    def add_entry_listener(
        self,
        key=None,
        predicate=None,
        added_func=None,
        removed_func=None,
        updated_func=None,
        evicted_func=None,
        clear_all_func=None,
    ):
        """Adds a continuous entry listener for this map.

        Listener will get notified for map events filtered with given parameters.

        Args:
            key: Key for filtering the events.
            predicate (hazelcast.predicate.Predicate): Predicate for filtering the events.
            added_func (function): Function to be called when an entry is added to map.
            removed_func (function): Function to be called when an entry is removed from map.
            updated_func (function): Function to be called when an entry is updated.
            evicted_func (function): Function to be called when an entry is evicted from map.
            clear_all_func (function): Function to be called when entries are cleared from map.

        Returns:
            hazelcast.future.Future[str]: A registration id which is used as a key to remove the listener.
        """
        if key and predicate:
            codec = replicated_map_add_entry_listener_to_key_with_predicate_codec
            key_data = self._to_data(key)
            predicate_data = self._to_data(predicate)
            request = codec.encode_request(self.name, key_data, predicate_data, self._is_smart)
        elif key and not predicate:
            codec = replicated_map_add_entry_listener_to_key_codec
            key_data = self._to_data(key)
            request = codec.encode_request(self.name, key_data, self._is_smart)
        elif not key and predicate:
            codec = replicated_map_add_entry_listener_with_predicate_codec
            predicate = self._to_data(predicate)
            request = codec.encode_request(self.name, predicate, self._is_smart)
        else:
            codec = replicated_map_add_entry_listener_codec
            request = codec.encode_request(self.name, self._is_smart)

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
            elif event.event_type == EntryEventType.UPDATED and updated_func:
                updated_func(event)
            elif event.event_type == EntryEventType.EVICTED and evicted_func:
                evicted_func(event)
            elif event.event_type == EntryEventType.CLEAR_ALL and clear_all_func:
                clear_all_func(event)

        return self._register_listener(
            request,
            lambda r: codec.decode_response(r),
            lambda reg_id: replicated_map_remove_entry_listener_codec.encode_request(
                self.name, reg_id
            ),
            lambda m: codec.handle(m, handle_event_entry),
        )

    def clear(self):
        """Wipes data out of the replicated map.

        Returns:
            hazelcast.future.Future[None]:
        """
        request = replicated_map_clear_codec.encode_request(self.name)
        return self._invoke(request)

    def contains_key(self, key):
        """Determines whether this map contains an entry with the key.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form of the key, not the
            actual implementations of ``__hash__`` and ``__eq__`` defined in key's class.

        Args:
            key: The specified key.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if this map contains an entry for the specified key,
            ``False`` otherwise.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        request = replicated_map_contains_key_codec.encode_request(self.name, key_data)
        return self._invoke_on_key(
            request, key_data, replicated_map_contains_key_codec.decode_response
        )

    def contains_value(self, value):
        """Determines whether this map contains one or more keys for the specified value.

        Args:
            value: The specified value.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if this map contains an entry for the specified value,
            ``False`` otherwise.
        """
        check_not_none(value, "value can't be None")
        value_data = self._to_data(value)
        request = replicated_map_contains_value_codec.encode_request(self.name, value_data)
        return self._invoke_on_partition(
            request, self._partition_id, replicated_map_contains_value_codec.decode_response
        )

    def entry_set(self):
        """Returns a List clone of the mappings contained in this map.

        Warning:
            The list is NOT backed by the map, so changes to the map are NOT reflected in the list, and vice-versa.

        Returns:
            hazelcast.future.Future[list[tuple]]: The list of key-value tuples in the map.
        """

        def handler(message):
            return ImmutableLazyDataList(
                replicated_map_entry_set_codec.decode_response(message), self._to_object
            )

        request = replicated_map_entry_set_codec.encode_request(self.name)
        return self._invoke_on_partition(request, self._partition_id, handler)

    def get(self, key):
        """Returns the value for the specified key, or ``None`` if this map does not contain this key.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form of the key, not the
            actual implementations of ``__hash__`` and ``__eq__`` defined in key's class.

        Args:
            key: The specified key.

        Returns:
            hazelcast.future.Future[any]: The value associated with the specified key.
        """
        check_not_none(key, "key can't be None")

        def handler(message):
            return self._to_object(replicated_map_get_codec.decode_response(message))

        key_data = self._to_data(key)
        request = replicated_map_get_codec.encode_request(self.name, key_data)
        return self._invoke_on_key(request, key_data, handler)

    def is_empty(self):
        """Returns ``True`` if this map contains no key-value mappings.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if this map contains no key-value mappings.
        """
        request = replicated_map_is_empty_codec.encode_request(self.name)
        return self._invoke_on_partition(
            request, self._partition_id, replicated_map_is_empty_codec.decode_response
        )

    def key_set(self):
        """Returns the list of keys in the ReplicatedMap.

        Warning:
            The list is NOT backed by the map, so changes to the map are NOT reflected in the list, and vice-versa.

        Returns:
            hazelcast.future.Future[list]: A list of the clone of the keys.
        """

        def handler(message):
            return ImmutableLazyDataList(
                replicated_map_key_set_codec.decode_response(message), self._to_object
            )

        request = replicated_map_key_set_codec.encode_request(self.name)
        return self._invoke_on_partition(request, self._partition_id, handler)

    def put(self, key, value, ttl=0):
        """Associates the specified value with the specified key in this map.

        If the map previously contained a mapping for the key, the old value is replaced by the specified value.
        If ttl is provided, entry will expire and get evicted after the ttl.

        Args:
            key: The specified key.
            value: The value to associate with the key.
            ttl (int): Maximum time in seconds for this entry to stay, if not provided, the value configured
                on server side configuration will be used.

        Returns:
            hazelcast.future.Future[any]: Previous value associated with key or ``None``
            if there was no mapping for key.
        """
        check_not_none(key, "key can't be None")
        check_not_none(key, "value can't be None")

        def handler(message):
            return self._to_object(replicated_map_put_codec.decode_response(message))

        key_data = self._to_data(key)
        value_data = self._to_data(value)
        request = replicated_map_put_codec.encode_request(
            self.name, key_data, value_data, to_millis(ttl)
        )
        return self._invoke_on_key(request, key_data, handler)

    def put_all(self, source):
        """Copies all of the mappings from the specified map to this map.

        No atomicity guarantees are given. In the case of a failure,
        some of the key-value tuples may get written, while others are not.

        Args:
            source (dict): Map which includes mappings to be stored in this map.

        Returns:
            hazelcast.future.Future[None]:
        """
        entries = []
        for key, value in six.iteritems(source):
            check_not_none(key, "key can't be None")
            check_not_none(value, "value can't be None")
            entries.append((self._to_data(key), self._to_data(value)))

        request = replicated_map_put_all_codec.encode_request(self.name, entries)
        return self._invoke(request)

    def remove(self, key):
        """Removes the mapping for a key from this map if it is present.

        The map will not contain a mapping for the specified key once the call returns.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form of the key, not the
            actual implementations of ``__hash__`` and ``__eq__`` defined in key's class.

        Args:
            key: Key of the mapping to be deleted.

        Returns:
            hazelcast.future.Future[any]: The previous value associated with key, or ``None``
            if there was no mapping for key.
        """
        check_not_none(key, "key can't be None")

        def handler(message):
            return self._to_object(replicated_map_remove_codec.decode_response(message))

        key_data = self._to_data(key)
        request = replicated_map_remove_codec.encode_request(self.name, key_data)
        return self._invoke_on_key(request, key_data, handler)

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
        request = replicated_map_size_codec.encode_request(self.name)
        return self._invoke_on_partition(
            request, self._partition_id, replicated_map_size_codec.decode_response
        )

    def values(self):
        """Returns the list of values in the map.

        Warning:
            The returned list is NOT backed by the map, so changes to the map are NOT reflected in the list, and
            vice-versa.

        Returns:
            hazelcast.future.Future[list]: The list of values in the map.
        """

        def handler(message):
            return ImmutableLazyDataList(
                replicated_map_values_codec.decode_response(message), self._to_object
            )

        request = replicated_map_values_codec.encode_request(self.name)
        return self._invoke_on_partition(request, self._partition_id, handler)
