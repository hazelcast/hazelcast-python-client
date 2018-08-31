import itertools

from hazelcast.future import combine_futures, ImmediateFuture
from hazelcast.near_cache import NearCache
from hazelcast.protocol.codec import map_add_entry_listener_codec, map_add_entry_listener_to_key_codec, \
    map_add_entry_listener_with_predicate_codec, map_add_entry_listener_to_key_with_predicate_codec, \
    map_add_index_codec, map_clear_codec, map_contains_key_codec, map_contains_value_codec, map_delete_codec, \
    map_entry_set_codec, map_entries_with_predicate_codec, map_evict_codec, map_evict_all_codec, map_flush_codec, \
    map_force_unlock_codec, map_get_codec, map_get_all_codec, map_get_entry_view_codec, map_is_empty_codec, \
    map_is_locked_codec, map_key_set_codec, map_key_set_with_predicate_codec, map_load_all_codec, \
    map_load_given_keys_codec, map_lock_codec, map_put_codec, map_put_all_codec, map_put_if_absent_codec, \
    map_put_transient_codec, map_size_codec, map_remove_codec, map_remove_if_same_codec, \
    map_remove_entry_listener_codec, map_replace_codec, map_replace_if_same_codec, map_set_codec, map_try_lock_codec, \
    map_try_put_codec, map_try_remove_codec, map_unlock_codec, map_values_codec, map_values_with_predicate_codec, \
    map_add_interceptor_codec, map_execute_on_all_keys_codec, map_execute_on_key_codec, map_execute_on_keys_codec, \
    map_execute_with_predicate_codec, map_add_near_cache_entry_listener_codec
from hazelcast.proxy.base import Proxy, EntryEvent, EntryEventType, get_entry_listener_flags, MAX_SIZE
from hazelcast.util import check_not_none, thread_id, to_millis
from hazelcast import six


class Map(Proxy):
    """
    Hazelcast Map client proxy to access the map on the cluster.

    Concurrent, distributed, observable and queryable map.

    This map can work both async(non-blocking) or sync(blocking). Blocking calls return the value of the call and block
    the execution until return value is calculated. However, async calls return :class:`~hazelcast.future.Future` and do not block execution.
    Result of the :class:`~hazelcast.future.Future` can be used whenever ready. A :class:`~hazelcast.future.Future`'s result can be obtained with blocking the execution by
    calling :func:`Future.result() <hazelcast.future.Future.result>`.

        Example of sync(blocking) usage:
            >>> #client is initialized Hazelcast Client.
            >>> my_map = client.get_map("map").blocking()# returns sync map, all map functions are blocking
            >>> print("map.put", my_map.put("key", "value"))
            >>> print("map.contains_key", my_map.contains_key("key"))
            >>> print("map.get", my_map.get("key"))
            >>> print("map.size", my_map.size())

        Example of async(non-blocking) usage:
            >>> #client is initialized Hazelcast Client.
            >>> my_map = client.get_map("map")
            >>> def put_callback(f):
            >>>     print("map.put", f.result())
            >>> my_map.put("key", "async_val").add_done_callback(put_callback)
            >>>
            >>> print("map.size", my_map.size().result())
            >>>
            >>> def contains_key_callback(f):
            >>>     print("map.contains_key", f.result())
            >>> my_map.contains_key("key").add_done_callback(contains_key_callback)

    This class does not allow ``None`` to be used as a key or value.
    """
    def __init__(self, client, service_name, name):
        super(Map, self).__init__(client, service_name, name)
        self.reference_id_generator = self._client.lock_reference_id_generator

    def add_entry_listener(self, include_value=False, key=None, predicate=None, added_func=None, removed_func=None, updated_func=None,
                           evicted_func=None, evict_all_func=None, clear_all_func=None, merged_func=None, expired_func=None):
        """
        Adds a continuous entry listener for this map. Listener will get notified for map events filtered with given
        parameters.

        :param include_value: (bool), whether received events include an old value or not (optional).
        :param key: (object), key for filtering the events (optional).
        :param predicate: (Predicate), predicate for filtering the events (optional).
        :param added_func: Function to be called when an entry is added to map (optional).
        :param removed_func: Function to be called when an entry is removed from map (optional).
        :param updated_func: Function to be called when an entry is updated (optional).
        :param evicted_func: Function to be called when an entry is evicted from map (optional).
        :param evict_all_func: Function to be called when entries are evicted from map (optional).
        :param clear_all_func: Function to be called when entries are cleared from map (optional).
        :param merged_func: Function to be called when WAN replicated entry is merged_func (optional).
        :param expired_func: Function to be called when an entry's live time is expired (optional).
        :return: (str), a registration id which is used as a key to remove the listener.

        .. seealso:: :class:`~hazelcast.serialization.predicate.Predicate` for more info about predicates.
        """
        flags = get_entry_listener_flags(added=added_func, removed=removed_func, updated=updated_func,
                                         evicted=evicted_func, evict_all=evict_all_func, clear_all=clear_all_func, merged=merged_func,
                                         expired=expired_func)

        if key and predicate:
            key_data = self._to_data(key)
            predicate_data = self._to_data(predicate)
            request = map_add_entry_listener_to_key_with_predicate_codec.encode_request(self.name, key_data,
                                                                                        predicate_data, include_value,
                                                                                        flags, False)
        elif key and not predicate:
            key_data = self._to_data(key)
            request = map_add_entry_listener_to_key_codec.encode_request(self.name, key_data, include_value, flags,
                                                                         False)
        elif not key and predicate:
            predicate = self._to_data(predicate)
            request = map_add_entry_listener_with_predicate_codec.encode_request(self.name, predicate, include_value,
                                                                                 flags, False)
        else:
            request = map_add_entry_listener_codec.encode_request(self.name, include_value, flags, False)

        def handle_event_entry(**_kwargs):
            event = EntryEvent(self._to_object, **_kwargs)
            if event.event_type == EntryEventType.added:
                added_func(event)
            elif event.event_type == EntryEventType.removed:
                removed_func(event)
            elif event.event_type == EntryEventType.updated:
                updated_func(event)
            elif event.event_type == EntryEventType.evicted:
                evicted_func(event)
            elif event.event_type == EntryEventType.evict_all:
                evict_all_func(event)
            elif event.event_type == EntryEventType.clear_all:
                clear_all_func(event)
            elif event.event_type == EntryEventType.merged:
                merged_func(event)
            elif event.event_type == EntryEventType.expired:
                expired_func(event)

        return self._start_listening(request, lambda m: map_add_entry_listener_codec.handle(m, handle_event_entry),
                                     lambda r: map_add_entry_listener_codec.decode_response(r)['response'])

    def add_index(self, attribute, ordered=False):
        """
        Adds an index to this map for the specified entries so that queries can run faster.

        Example:
            Let's say your map values are Employee objects.
                >>> class Employee(IdentifiedDataSerializable):
                >>>     active = false
                >>>     age = None
                >>>     name = None
                >>>     #other fields
                >>>
                >>>     #methods

            If you query your values mostly based on age and active fields, you should consider indexing these.
                >>> map = self.client.get_map("employees")
                >>> map.add_index("age" , true) #ordered, since we have ranged queries for this field
                >>> map.add_index("active", false) #not ordered, because boolean field cannot have range


        :param attribute: (str), index attribute of the value.
        :param ordered: (bool), for ordering the index or not (optional).
        """
        return self._encode_invoke(map_add_index_codec, attribute=attribute, ordered=ordered)

    def add_interceptor(self, interceptor):
        """
        Adds an interceptor for this map. Added interceptor will intercept operations and execute user defined methods.

        :param interceptor: (object), interceptor for the map which includes user defined methods.
        :return: (str),id of registered interceptor.
        """
        return self._encode_invoke(map_add_interceptor_codec, interceptor=self._to_data(interceptor))

    def clear(self):
        """
        Clears the map.

        The MAP_CLEARED event is fired for any registered listeners.
        """
        return self._encode_invoke(map_clear_codec)

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
        return self._contains_key_internal(key_data)

    def contains_value(self, value):
        """
        Determines whether this map contains one or more keys for the specified value.

        :param value: (object), the specified value.
        :return: (bool), ``true`` if this map contains an entry for the specified value.
        """
        check_not_none(value, "value can't be None")
        value_data = self._to_data(value)
        return self._encode_invoke(map_contains_value_codec, value=value_data)

    def delete(self, key):
        """
        Removes the mapping for a key from this map if it is present (optional operation).

        Unlike remove(object), this operation does not return the removed value, which avoids the serialization cost of
        the returned value. If the removed value will not be used, a delete operation is preferred over a remove
        operation for better performance.

        The map will not contain a mapping for the specified key once the call returns.

        **Warning:
        This method breaks the contract of EntryListener. When an entry is removed by delete(), it fires an EntryEvent
        with a** ``None`` **oldValue. Also, a listener with predicates will have** ``None`` **values, so only the keys can be queried
        via predicates.**

        :param key: (object), key of the mapping to be deleted.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._delete_internal(key_data)

    def entry_set(self, predicate=None):
        """
        Returns a list clone of the mappings contained in this map.

        **Warning:
        The list is NOT backed by the map, so changes to the map are NOT reflected in the list, and vice-versa.**

        :param predicate: (Predicate), predicate for the map to filter entries (optional).
        :return: (Sequence), the list of key-value tuples in the map.

        .. seealso:: :class:`~hazelcast.serialization.predicate.Predicate` for more info about predicates.
        """
        if predicate:
            predicate_data = self._to_data(predicate)
            return self._encode_invoke(map_entries_with_predicate_codec, predicate=predicate_data)
        else:
            return self._encode_invoke(map_entry_set_codec)

    def evict(self, key):
        """
        Evicts the specified key from this map.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), key to evict.
        :return: (bool), ``true`` if the key is evicted, ``false`` otherwise.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._evict_internal(key_data)

    def evict_all(self):
        """
        Evicts all keys from this map except the locked ones.

        The EVICT_ALL event is fired for any registered listeners.
        """
        return self._encode_invoke(map_evict_all_codec)

    def execute_on_entries(self, entry_processor, predicate=None):
        """
        Applies the user defined EntryProcessor to all the entries in the map or entries in the map which satisfies
        the predicate if provided. Returns the results mapped by each key in the map.

        :param entry_processor: (object), A stateful serializable object which represents the EntryProcessor defined on
            server side.
            This object must have a serializable EntryProcessor counter part registered on server side with the actual
            ``org.hazelcast.map.EntryProcessor`` implementation.
        :param predicate: (Predicate), predicate for filtering the entries (optional).
        :return: (Sequence), list of map entries which includes the keys and the results of the entry process.

        .. seealso:: :class:`~hazelcast.serialization.predicate.Predicate` for more info about predicates.
        """
        if predicate:
            return self._encode_invoke(map_execute_with_predicate_codec, entry_processor=self._to_data(entry_processor),
                                       predicate=self._to_data(predicate))
        return self._encode_invoke(map_execute_on_all_keys_codec, entry_processor=self._to_data(entry_processor))

    def execute_on_key(self, key, entry_processor):
        """
        Applies the user defined EntryProcessor to the entry mapped by the key. Returns the object which is the result
        of EntryProcessor's process method.

        :param key: (object), specified key for the entry to be processed.
        :param entry_processor: (object), A stateful serializable object which represents the EntryProcessor defined on
            server side.
            This object must have a serializable EntryProcessor counter part registered on server side with the actual
            ``org.hazelcast.map.EntryProcessor`` implementation.
        :return: (object), result of entry process.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._execute_on_key_internal(key_data, entry_processor)

    def execute_on_keys(self, keys, entry_processor):
        """
        Applies the user defined EntryProcessor to the entries mapped by the collection of keys. Returns the results
        mapped by each key in the collection.

        :param keys: (Collection), collection of the keys for the entries to be processed.
        :param entry_processor: (object), A stateful serializable object which represents the EntryProcessor defined on
            server side.
            This object must have a serializable EntryProcessor counter part registered on server side with the actual
            ``org.hazelcast.map.EntryProcessor`` implementation.
        :return: (Sequence), list of map entries which includes the keys and the results of the entry process.
        """
        key_list = []
        for key in keys:
            check_not_none(key, "key can't be None")
            key_list.append(self._to_data(key))

        return self._encode_invoke(map_execute_on_keys_codec, entry_processor=self._to_data(entry_processor),
                                   keys=key_list)

    def flush(self):
        """
        Flushes all the local dirty entries.
        """
        return self._encode_invoke(map_flush_codec)

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
        return self._encode_invoke_on_key(map_force_unlock_codec, key_data, key=key_data,
                                          reference_id=self.reference_id_generator.get_next())

    def get(self, key):
        """
        Returns the value for the specified key, or ``None`` if this map does not contain this key.

        **Warning:
        This method returns a clone of original value, modifying the returned value does not change the actual value in
        the map. One should put modified value back to make changes visible to all nodes.**
            >>> value = map.get(key)
            >>> value.update_some_property()
            >>> map.put(key,value)

        **Warning 2: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), the specified key.
        :return: (object), the value for the specified key.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._get_internal(key_data)

    def get_all(self, keys):
        """
        Returns the entries for the given keys.

        **Warning:
        The returned map is NOT backed by the original map, so changes to the original map are NOT reflected in the
        returned map, and vice-versa.**

        **Warning 2: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param keys: (Collection), keys to get.
        :return: (dict), dictionary of map entries.
        """
        check_not_none(keys, "keys can't be None")
        if not keys:
            return ImmediateFuture({})

        partition_service = self._client.partition_service
        partition_to_keys = {}

        for key in keys:
            check_not_none(key, "key can't be None")
            key_data = self._to_data(key)
            partition_id = partition_service.get_partition_id(key_data)
            try:
                partition_to_keys[partition_id][key] = key_data
            except KeyError:
                partition_to_keys[partition_id] = {key: key_data}

        return self._get_all_internal(partition_to_keys)

    def get_entry_view(self, key):
        """
        Returns the EntryView for the specified key.

        **Warning:
        This method returns a clone of original mapping, modifying the returned value does not change the actual value
        in the map. One should put modified value back to make changes visible to all nodes.**

        **Warning 2: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), the key of the entry.
        :return: (EntryView), EntryView of the specified key.

        .. seealso:: :class:`~hazelcast.core.EntryView` for more info about EntryView.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(map_get_entry_view_codec, key_data, key=key_data, thread_id=thread_id())

    def is_empty(self):
        """
        Returns ``true`` if this map contains no key-value mappings.

        :return: (bool), ``true`` if this map contains no key-value mappings.
        """
        return self._encode_invoke(map_is_empty_codec)

    def is_locked(self, key):
        """
        Checks the lock for the specified key. If the lock is acquired, it returns ``true``. Otherwise, it returns ``false``.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), the key that is checked for lock
        :return: (bool), ``true`` if lock is acquired, ``false`` otherwise.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(map_is_locked_codec, key_data, key=key_data)

    def key_set(self, predicate=None):
        """
        Returns a List clone of the keys contained in this map or the keys of the entries filtered with the predicate if
        provided.

        **Warning:
        The list is NOT backed by the map, so changes to the map are NOT reflected in the list, and vice-versa.**

        :param predicate: (Predicate), predicate to filter the entries (optional).
        :return: (Sequence), a list of the clone of the keys.

        .. seealso:: :class:`~hazelcast.serialization.predicate.Predicate` for more info about predicates.
        """
        if predicate:
            predicate_data = self._to_data(predicate)
            return self._encode_invoke(map_key_set_with_predicate_codec, predicate=predicate_data)
        else:
            return self._encode_invoke(map_key_set_codec)

    def load_all(self, keys=None, replace_existing_values=True):
        """
        Loads all keys from the store at server side or loads the given keys if provided.

        :param keys: (Collection), keys of the entry values to load (optional).
        :param replace_existing_values: (bool), whether the existing values will be replaced or not with those loaded
        from the server side MapLoader (optional).
        """
        if keys:
            key_data_list = list(map(self._to_data, keys))
            return self._load_all_internal(key_data_list, replace_existing_values)
        else:
            return self._encode_invoke(map_load_all_codec, replace_existing_values=replace_existing_values)

    def lock(self, key, ttl=-1):
        """
        Acquires the lock for the specified key infinitely or for the specified lease time if provided.

        If the lock is not available, the current thread becomes disabled for thread scheduling purposes and lies
        dormant until the lock has been acquired.

        You get a lock whether the value is present in the map or not. Other threads (possibly on other systems) would
        block on their invoke of lock() until the non-existent key is unlocked. If the lock holder introduces the key to
        the map, the put() operation is not blocked. If a thread not holding a lock on the non-existent key tries to
        introduce the key while a lock exists on the non-existent key, the put() operation blocks until it is unlocked.

        Scope of the lock is this map only. Acquired lock is only for the key in this map.

        Locks are re-entrant; so, if the key is locked N times, it should be unlocked N times before another thread can
        acquire it.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), the key to lock.
        :param ttl: (int), time in seconds to wait before releasing the lock (optional).
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(map_lock_codec, key_data, invocation_timeout=MAX_SIZE, key=key_data,
                                          thread_id=thread_id(), ttl=to_millis(ttl),
                                          reference_id=self.reference_id_generator.get_next())

    def put(self, key, value, ttl=-1):
        """
        Associates the specified value with the specified key in this map. If the map previously contained a mapping for
        the key, the old value is replaced by the specified value. If ttl is provided, entry will expire and get evicted
        after the ttl.

        **Warning:
        This method returns a clone of the previous value, not the original (identically equal) value previously put
        into the map.**

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), the specified key.
        :param value: (object), the value to associate with the key.
        :param ttl: (int), maximum time in seconds for this entry to stay, if not provided, the value configured on
            server side configuration will be used(optional).
        :return: (object), previous value associated with key or ``None`` if there was no mapping for key.
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._put_internal(key_data, value_data, ttl)

    def put_all(self, map):
        """
        Copies all of the mappings from the specified map to this map. No atomicity guarantees are
        given. In the case of a failure, some of the key-value tuples may get written, while others are not.

        :param map: (dict), map which includes mappings to be stored in this map.
        """
        check_not_none(map, "map can't be None")
        if not map:
            return ImmediateFuture(None)

        partition_service = self._client.partition_service
        partition_map = {}

        for key, value in six.iteritems(map):
            check_not_none(key, "key can't be None")
            check_not_none(value, "value can't be None")
            entry = (self._to_data(key), self._to_data(value))
            partition_id = partition_service.get_partition_id(entry[0])
            try:
                partition_map[partition_id].append(entry)
            except KeyError:
                partition_map[partition_id] = [entry]

        futures = []
        for partition_id, entry_list in six.iteritems(partition_map):
            future = self._encode_invoke_on_partition(map_put_all_codec, partition_id, entries=dict(entry_list))
            futures.append(future)

        return combine_futures(*futures)

    def put_if_absent(self, key, value, ttl=-1):
        """
        Associates the specified key with the given value if it is not already associated. If ttl is provided, entry
        will expire and get evicted after the ttl.

        This is equivalent to:
            >>> if not map.contains_key(key):
            >>>     return map.put(key,value)
            >>> else:
            >>>     return map.get(key)
        except that the action is performed atomically.

        **Warning:
        This method returns a clone of the previous value, not the original (identically equal) value previously put
        into the map.**

        **Warning 2: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), key of the entry.
        :param value: (object), value of the entry.
        :param ttl:  (int), maximum time in seconds for this entry to stay in the map, if not provided, the value
            configured on server side configuration will be used (optional).
        :return: (object), old value of the entry.
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")

        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._put_if_absent_internal(key_data, value_data, ttl)

    def put_transient(self, key, value, ttl=-1):
        """
        Same as put(TKey, TValue, Ttl), but MapStore defined at the server side will not be called.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), key of the entry.
        :param value: (object), value of the entry.
        :param ttl: (int), maximum time in seconds for this entry to stay in the map, if not provided, the value
            configured on server side configuration will be used (optional).
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")

        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._put_transient_internal(key_data, value_data, ttl)

    def remove(self, key):
        """
        Removes the mapping for a key from this map if it is present. The map will not contain a mapping for the
        specified key once the call returns.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), key of the mapping to be deleted.
        :return: (object), the previous value associated with key, or ``None`` if there was no mapping for key.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._remove_internal(key_data)

    def remove_if_same(self, key, value):
        """
        Removes the entry for a key only if it is currently mapped to a given value.

        This is equivalent to:
            >>> if map.contains_key(key) and map.get(key).equals(value):
            >>>     map.remove(key)
            >>>     return true
            >>> else:
            >>>     return false
        except that the action is performed atomically.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), the specified key.
        :param value: (object), remove the key if it has this value.
        :return: (bool), ``true`` if the value was removed.
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")

        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._remove_if_same_internal_(key_data, value_data)

    def remove_entry_listener(self, registration_id):
        """
        Removes the specified entry listener. Returns silently if there is no such listener added before.

        :param registration_id: (str), id of registered listener.
        :return: (bool), ``true`` if registration is removed, ``false`` otherwise.
        """
        return self._stop_listening(registration_id,
                                    lambda i: map_remove_entry_listener_codec.encode_request(self.name, i))

    def replace(self, key, value):
        """
        Replaces the entry for a key only if it is currently mapped to some value.

        This is equivalent to:
            >>> if map.contains_key(key):
            >>>     return map.put(key,value)
            >>> else:
            >>>     return None
        except that the action is performed atomically.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        **Warning 2:
        This method returns a clone of the previous value, not the original (identically equal) value previously put
        into the map.**

        :param key: (object), the specified key.
        :param value: (object), the value to replace the previous value.
        :return: (object), previous value associated with key, or ``None`` if there was no mapping for key.
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")

        key_data = self._to_data(key)
        value_data = self._to_data(value)

        return self._replace_internal(key_data, value_data)

    def replace_if_same(self, key, old_value, new_value):
        """
        Replaces the entry for a key only if it is currently mapped to a given value.

        This is equivalent to:
            >>> if map.contains_key(key) and map.get(key).equals(old_value):
            >>>     map.put(key, new_value)
            >>>     return true
            >>> else:
            >>>     return false
        except that the action is performed atomically.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), the specified key.
        :param old_value: (object), replace the key value if it is the old value.
        :param new_value: (object), the new value to replace the old value.
        :return: (bool), ``true`` if the value was replaced.
        """
        check_not_none(key, "key can't be None")
        check_not_none(old_value, "old_value can't be None")
        check_not_none(new_value, "new_value can't be None")

        key_data = self._to_data(key)
        old_value_data = self._to_data(old_value)
        new_value_data = self._to_data(new_value)

        return self._replace_if_same_internal(key_data, old_value_data, new_value_data)

    def set(self, key, value, ttl=-1):
        """
        Puts an entry into this map. Similar to the put operation except that set doesn't return the old value, which is
        more efficient. If ttl is provided, entry will expire and get evicted after the ttl.

        **Warning: This method uses __hash__ and __eq__ methods of binary form of the key, not the actual implementations
        of __hash__ and __eq__ defined in key's class.**

        :param key: (object), key of the entry.
        :param value: (object), value of the entry.
        :param ttl: (int), maximum time in seconds for this entry to stay in the map, 0 means infinite. If ttl is not
            provided, the value configured on server side configuration will be used (optional).
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._set_internal(key_data, value_data, ttl)

    def size(self):
        """
        Returns the number of entries in this map.

        :return: (int), number of entries in this map.
        """
        return self._encode_invoke(map_size_codec)

    def try_lock(self, key, ttl=-1, timeout=0):
        """
        Tries to acquire the lock for the specified key. When the lock is not available,

            * If timeout is not provided, the current thread doesn't wait and returns ``false`` immediately.
            * If a timeout is provided, the current thread becomes disabled for thread scheduling purposes and lies
              dormant until one of the followings happens:
                * the lock is acquired by the current thread, or
                * the specified waiting time elapses.

        If ttl is provided, lock will be released after this time elapses.

        :param key: (object), key to lock in this map.
        :param ttl: (int), time in seconds to wait before releasing the lock (optional).
        :param timeout: (int), maximum time in seconds to wait for the lock (optional).
        :return: (bool), ``true`` if the lock was acquired and otherwise, ``false``.
        """
        check_not_none(key, "key can't be None")

        key_data = self._to_data(key)

        return self._encode_invoke_on_key(map_try_lock_codec, key_data, invocation_timeout=MAX_SIZE, key=key_data,
                                          thread_id=thread_id(), lease=to_millis(ttl), timeout=to_millis(timeout),
                                          reference_id=self.reference_id_generator.get_next())

    def try_put(self, key, value, timeout=0):
        """
        Tries to put the given key and value into this map and returns immediately if timeout is not provided. If
        timeout is provided, operation waits until it is completed or timeout is reached.

        :param key: (object), key of the entry.
        :param value: (object), value of the entry.
        :param timeout: (int), operation timeout in seconds (optional).
        :return: (bool), ``true`` if the put is successful, ``false`` otherwise.
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")

        key_data = self._to_data(key)
        value_data = self._to_data(value)

        return self._try_put_internal(key_data, value_data, timeout)

    def try_remove(self, key, timeout=0):
        """
        Tries to remove the given key from this map and returns immediately if timeout is not provided. If
        timeout is provided, operation waits until it is completed or timeout is reached.

        :param key: (object), key of the entry to be deleted.
        :param timeout: (int), operation timeout in seconds (optional).
        :return: (bool), ``true`` if the remove is successful, ``false`` otherwise.
        """
        check_not_none(key, "key can't be None")

        key_data = self._to_data(key)
        return self._try_remove_internal(key_data, timeout)

    def unlock(self, key):
        """
        Releases the lock for the specified key. It never blocks and returns immediately. If the current thread is the
        holder of this lock, then the hold count is decremented. If the hold count is zero, then the lock is released.

        :param key: (object), the key to lock.
        """
        check_not_none(key, "key can't be None")

        key_data = self._to_data(key)

        return self._encode_invoke_on_key(map_unlock_codec, key_data, key=key_data, thread_id=thread_id(),
                                          reference_id=self.reference_id_generator.get_next())

    def values(self, predicate=None):
        """
        Returns a list clone of the values contained in this map or values of the entries which are filtered with
        the predicate if provided.

        **Warning:
        The list is NOT backed by the map, so changes to the map are NOT reflected in the list, and
        vice-versa.**

        :param predicate: (Predicate), predicate to filter the entries (optional).
        :return: (Sequence), a list of clone of the values contained in this map.

        .. seealso:: :class:`~hazelcast.serialization.predicate.Predicate` for more info about predicates.
        """
        if predicate:
            predicate_data = self._to_data(predicate)
            return self._encode_invoke(map_values_with_predicate_codec, predicate=predicate_data)
        else:
            return self._encode_invoke(map_values_codec)

    # internals
    def _contains_key_internal(self, key_data):
        return self._encode_invoke_on_key(map_contains_key_codec, key_data, key=key_data, thread_id=thread_id())

    def _get_internal(self, key_data):
        return self._encode_invoke_on_key(map_get_codec, key_data, key=key_data, thread_id=thread_id())

    def _get_all_internal(self, partition_to_keys, futures=None):
        if futures is None:
            futures = []
        for partition_id, key_dict in six.iteritems(partition_to_keys):
            future = self._encode_invoke_on_partition(map_get_all_codec, partition_id, keys=list(key_dict.values()))
            futures.append(future)

        def merge(f):
            return dict(itertools.chain(*f.result()))

        return combine_futures(*futures).continue_with(merge)

    def _remove_internal(self, key_data):
        return self._encode_invoke_on_key(map_remove_codec, key_data, key=key_data, thread_id=thread_id())

    def _remove_if_same_internal_(self, key_data, value_data):
        return self._encode_invoke_on_key(map_remove_if_same_codec, key_data, key=key_data, value=value_data,
                                          thread_id=thread_id())

    def _delete_internal(self, key_data):
        return self._encode_invoke_on_key(map_delete_codec, key_data, key=key_data, thread_id=thread_id())

    def _put_internal(self, key_data, value_data, ttl):
        return self._encode_invoke_on_key(map_put_codec, key_data, key=key_data, value=value_data, thread_id=thread_id(),
                                          ttl=to_millis(ttl))

    def _set_internal(self, key_data, value_data, ttl):
        return self._encode_invoke_on_key(map_set_codec, key_data, key=key_data, value=value_data, thread_id=thread_id(),
                                          ttl=to_millis(ttl))

    def _try_remove_internal(self, key_data, timeout):
        return self._encode_invoke_on_key(map_try_remove_codec, key_data, key=key_data, thread_id=thread_id(),
                                          timeout=to_millis(timeout))

    def _try_put_internal(self, key_data, value_data, timeout):
        return self._encode_invoke_on_key(map_try_put_codec, key_data, key=key_data, value=value_data,
                                          thread_id=thread_id(), timeout=to_millis(timeout))

    def _put_transient_internal(self, key_data, value_data, ttl):
        return self._encode_invoke_on_key(map_put_transient_codec, key_data, key=key_data, value=value_data,
                                          thread_id=thread_id(), ttl=to_millis(ttl))

    def _put_if_absent_internal(self, key_data, value_data, ttl):
        return self._encode_invoke_on_key(map_put_if_absent_codec, key_data, key=key_data, value=value_data,
                                          thread_id=thread_id(), ttl=to_millis(ttl))

    def _replace_if_same_internal(self, key_data, old_value_data, new_value_data):
        return self._encode_invoke_on_key(map_replace_if_same_codec, key_data, key=key_data, test_value=old_value_data,
                                          value=new_value_data, thread_id=thread_id())

    def _replace_internal(self, key_data, value_data):
        return self._encode_invoke_on_key(map_replace_codec, key_data, key=key_data, value=value_data, thread_id=thread_id())

    def _evict_internal(self, key_data):
        return self._encode_invoke_on_key(map_evict_codec, key_data, key=key_data, thread_id=thread_id())

    def _load_all_internal(self, key_data_list, replace_existing_values):
        return self._encode_invoke(map_load_given_keys_codec, keys=key_data_list, replace_existing_values=replace_existing_values)

    def _execute_on_key_internal(self, key_data, entry_processor):
        return self._encode_invoke_on_key(map_execute_on_key_codec, key_data, key=key_data,
                                          entry_processor=self._to_data(entry_processor), thread_id=thread_id())


class MapFeatNearCache(Map):
    """
    Map proxy implementation featuring Near Cache
    """
    def __init__(self, client, service_name, name):
        super(MapFeatNearCache, self).__init__(client, service_name, name)
        near_cache_config = client.config.near_cache_configs.get(name, None)
        if near_cache_config is None:
            raise ValueError("NearCache config cannot be None here!")
        self._invalidation_listener_id = None
        self._near_cache = create_near_cache(client.serialization_service, near_cache_config)
        if near_cache_config.invalidate_on_change:
            self._add_near_cache_invalidation_listener()

    def clear(self):
        self._near_cache.clear()
        return super(MapFeatNearCache, self).clear()

    def evict_all(self):
        self._near_cache.clear()
        return super(MapFeatNearCache, self).evict_all()

    def load_all(self, keys=None, replace_existing_values=True):
        if keys is None and replace_existing_values:
            self._near_cache.clear()
        return super(MapFeatNearCache, self).load_all(keys, replace_existing_values)

    def _on_destroy(self):
        self._remove_near_cache_invalidation_listener()
        self._near_cache.clear()
        super(MapFeatNearCache, self)._on_destroy()

    def _add_near_cache_invalidation_listener(self):
        def handle(message):
            map_add_near_cache_entry_listener_codec.handle(message, self._handle_invalidation, self._handle_batch_invalidation)

        def handle_decode(message):
            return map_add_near_cache_entry_listener_codec.decode_response(message)['response']

        try:
            request = map_add_near_cache_entry_listener_codec.encode_request(self.name, EntryEventType.invalidation, False)
            self._invalidation_listener_id = self._start_listening(request, handle, handle_decode)
        except:
            self.logger.severe("-----------------\n Near Cache is not initialized!!! \n-----------------")

    def _remove_near_cache_invalidation_listener(self):
        if self._invalidation_listener_id:
            self.remove_entry_listener(self._invalidation_listener_id)

    def _handle_invalidation(self, key):
        # key is always ``Data``
        # null key means near cache has to remove all entries in it.
        # see MapAddNearCacheEntryListenerMessageTask.
        if key is None:
            self._near_cache.clear()
        else:
            self._invalidate_cache(key)

    def _handle_batch_invalidation(self, keys):
        # key_list is always list of ``Data``
        for key_data in keys:
            self._invalidate_cache(key_data)

    def _invalidate_cache(self, key_data):
        try:
            del self._near_cache[key_data]
        except KeyError:
            # There is nothing to invalidate
            pass

    def _invalidate_cache_batch(self, key_data_list):
        for key_data in key_data_list:
            try:
                del self._near_cache[key_data]
            except KeyError:
                # There is nothing to invalidate
                pass

    # internals
    def _contains_key_internal(self, key_data):
        try:
            return self._near_cache[key_data]
        except KeyError:
            return super(MapFeatNearCache, self)._contains_key_internal(key_data)

    def _get_internal(self, key_data):
        try:
            value = self._near_cache[key_data]
            return ImmediateFuture(value)
        except KeyError:
            future = super(MapFeatNearCache, self)._get_internal(key_data)
            return future.continue_with(self._update_cache, key_data)

    def _update_cache(self, f, key_data):
        self._near_cache.__setitem__(key_data, f.result())
        return f.result()

    def _get_all_internal(self, partition_to_keys, futures=None):
        if futures is None:
            futures = []
        for key_dic in six.itervalues(partition_to_keys):
            for key in list(key_dic.keys()):
                try:
                    key_data = key_dic[key]
                    value = self._near_cache[key_data]
                    future = ImmediateFuture((key, value))
                    futures.append(future)
                    del key_dic[key]
                except KeyError:
                    pass
        return super(MapFeatNearCache, self)._get_all_internal(partition_to_keys, futures)

    def _try_remove_internal(self, key_data, timeout):
        self._invalidate_cache(key_data)
        return super(MapFeatNearCache, self)._try_remove_internal(key_data, timeout)

    def _try_put_internal(self, key_data, value_data, timeout):
        self._invalidate_cache(key_data)
        return super(MapFeatNearCache, self)._try_put_internal(key_data, value_data, timeout)

    def _set_internal(self, key_data, value_data, ttl):
        self._invalidate_cache(key_data)
        return super(MapFeatNearCache, self)._set_internal(key_data, value_data, ttl)

    def _replace_internal(self, key_data, value_data):
        self._invalidate_cache(key_data)
        return super(MapFeatNearCache, self)._replace_internal(key_data, value_data)

    def _replace_if_same_internal(self, key_data, old_value_data, new_value_data):
        self._invalidate_cache(key_data)
        return super(MapFeatNearCache, self)._replace_if_same_internal(key_data, old_value_data, new_value_data)

    def _remove_internal(self, key_data):
        self._invalidate_cache(key_data)
        return super(MapFeatNearCache, self)._remove_internal(key_data)

    def _remove_if_same_internal_(self, key_data, value_data):
        self._invalidate_cache(key_data)
        return super(MapFeatNearCache, self)._remove_if_same_internal_(key_data, value_data)

    def _put_transient_internal(self, key_data, value_data, ttl):
        self._invalidate_cache(key_data)
        return super(MapFeatNearCache, self)._put_transient_internal(key_data, value_data, ttl)

    def _put_internal(self, key_data, value_data, ttl):
        self._invalidate_cache(key_data)
        return super(MapFeatNearCache, self)._put_internal(key_data, value_data, ttl)

    def _put_if_absent_internal(self, key_data, value_data, ttl):
        self._invalidate_cache(key_data)
        return super(MapFeatNearCache, self)._put_if_absent_internal(key_data, value_data, ttl)

    def _load_all_internal(self, key_data_list, replace_existing_values):
        self._invalidate_cache_batch(key_data_list)
        return super(MapFeatNearCache, self)._load_all_internal(key_data_list, replace_existing_values)

    def _execute_on_key_internal(self, key_data, entry_processor):
        self._invalidate_cache(key_data)
        return super(MapFeatNearCache, self)._execute_on_key_internal(key_data, entry_processor)

    def _evict_internal(self, key_data):
        self._invalidate_cache(key_data)
        return super(MapFeatNearCache, self)._evict_internal(key_data)

    def _delete_internal(self, key_data):
        self._invalidate_cache(key_data)
        return super(MapFeatNearCache, self)._delete_internal(key_data)


def create_near_cache(serialization_service, near_cache_config):
    return NearCache(serialization_service,
                     near_cache_config.in_memory_format,
                     near_cache_config.time_to_live_seconds,
                     near_cache_config.max_idle_seconds,
                     near_cache_config.invalidate_on_change,
                     near_cache_config.eviction_policy,
                     near_cache_config.eviction_max_size,
                     near_cache_config.eviction_sampling_count,
                     near_cache_config.eviction_sampling_pool_size)


def create_map_proxy(client, service_name, name, **kwargs):
    near_cache_config = client.config.near_cache_configs.get(name, None)
    if near_cache_config is None:
        return Map(client=client, service_name=service_name, name=name)
    else:
        return MapFeatNearCache(client=client, service_name=service_name, name=name)
