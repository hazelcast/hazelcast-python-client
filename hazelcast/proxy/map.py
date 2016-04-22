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
from hazelcast.proxy.base import Proxy, EntryEvent, EntryEventType, get_entry_listener_flags
from hazelcast.util import check_not_none, thread_id, to_millis


class Map(Proxy):
    def add_entry_listener(self, include_value=False, key=None, predicate=None, added=None, removed=None, updated=None,
                           evicted=None, evict_all=None, clear_all=None, merged=None, expired=None):
        flags = get_entry_listener_flags(added=added, removed=removed, updated=updated,
                                         evicted=evicted, evict_all=evict_all, clear_all=clear_all, merged=merged,
                                         expired=expired)

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
                added(event)
            elif event.event_type == EntryEventType.removed:
                removed(event)
            elif event.event_type == EntryEventType.updated:
                updated(event)
            elif event.event_type == EntryEventType.evicted:
                evicted(event)
            elif event.event_type == EntryEventType.evict_all:
                evict_all(event)
            elif event.event_type == EntryEventType.clear_all:
                clear_all(event)
            elif event.event_type == EntryEventType.merged:
                merged(event)
            elif event.event_type == EntryEventType.expired:
                expired(event)

        return self._start_listening(request, lambda m: map_add_entry_listener_codec.handle(m, handle_event_entry),
                                     lambda r: map_add_entry_listener_codec.decode_response(r)['response'])

    def add_index(self, attribute, ordered=False):
        return self._encode_invoke(map_add_index_codec, attribute=attribute, ordered=ordered)

    def add_interceptor(self, interceptor):
        return self._encode_invoke(map_add_interceptor_codec, interceptor=self._to_data(interceptor))

    def clear(self):
        return self._encode_invoke(map_clear_codec)

    def contains_key(self, key):
        """
        :param key:
        :return:
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._contains_key_internal(key_data)

    def contains_value(self, value):
        check_not_none(value, "value can't be None")
        value_data = self._to_data(value)
        return self._encode_invoke(map_contains_value_codec, value=value_data)

    def delete(self, key):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._delete_internal(key_data)

    def entry_set(self, predicate=None):
        if predicate:
            predicate_data = self._to_data(predicate)
            return self._encode_invoke(map_entries_with_predicate_codec, predicate=predicate_data)
        else:
            return self._encode_invoke(map_entry_set_codec)

    def evict(self, key):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._evict_internal(key_data)

    def evict_all(self):
        return self._encode_invoke(map_evict_all_codec)

    def execute_on_entries(self, entry_processor, predicate=None):
        if predicate:
            return self._encode_invoke(map_execute_with_predicate_codec, entry_processor=self._to_data(entry_processor),
                                       predicate=self._to_data(predicate))
        return self._encode_invoke(map_execute_on_all_keys_codec, entry_processor=self._to_data(entry_processor))

    def execute_on_key(self, key, entry_processor):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._execute_on_key_internal(key_data, entry_processor)

    def execute_on_keys(self, keys, entry_processor):
        key_list = []
        for key in keys:
            check_not_none(key, "key can't be None")
            key_list.append(self._to_data(key))

        return self._encode_invoke(map_execute_on_keys_codec, entry_processor=self._to_data(entry_processor),
                                   keys=key_list)

    def flush(self):
        return self._encode_invoke(map_flush_codec)

    def force_unlock(self, key):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(map_force_unlock_codec, key_data, key=key_data)

    def get(self, key):
        """
        :param key:
        :return:
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._get_internal(key_data)

    def get_all(self, keys):
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
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(map_get_entry_view_codec, key_data, key=key_data, thread_id=thread_id())

    def is_empty(self):
        return self._encode_invoke(map_is_empty_codec)

    def is_locked(self, key):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(map_is_locked_codec, key_data, key=key_data)

    def key_set(self, predicate=None):
        if predicate:
            predicate_data = self._to_data(predicate)
            return self._encode_invoke(map_key_set_with_predicate_codec, predicate=predicate_data)
        else:
            return self._encode_invoke(map_key_set_codec)

    def load_all(self, keys=None, replace_existing_values=True):
        if keys:
            key_data_list = map(self._to_data, keys)
            return self._load_all_internal(key_data_list, replace_existing_values)
        else:
            return self._encode_invoke(map_load_all_codec, replace_existing_values=replace_existing_values)

    def lock(self, key, ttl=-1):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(map_lock_codec, key_data, key=key_data, thread_id=thread_id(), ttl=to_millis(ttl))

    def put(self, key, value, ttl=-1):
        """
        :param key:
        :param value:
        :param ttl:
        :return:
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._put_internal(key_data, value_data, ttl)

    def put_all(self, map):
        check_not_none(map, "map can't be None")
        if not map:
            return ImmediateFuture(None)

        partition_service = self._client.partition_service
        partition_map = {}

        for key, value in map.iteritems():
            check_not_none(key, "key can't be None")
            check_not_none(value, "value can't be None")
            entry = (self._to_data(key), self._to_data(value))
            partition_id = partition_service.get_partition_id(entry[0])
            try:
                partition_map[partition_id].append(entry)
            except KeyError:
                partition_map[partition_id] = [entry]

        futures = []
        for partition_id, entry_list in partition_map.iteritems():
            future = self._encode_invoke_on_partition(map_put_all_codec, partition_id, entries=dict(entry_list))
            futures.append(future)

        return combine_futures(*futures)

    def put_if_absent(self, key, value, ttl=-1):
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")

        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._put_if_absent_internal(key_data, value_data, ttl)

    def put_transient(self, key, value, ttl=-1):
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")

        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._put_transient_internal(key_data, value_data, ttl)

    def remove(self, key):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._remove_internal(key_data)

    def remove_if_same(self, key, value):
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")

        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._remove_if_same_internal_(key_data, value_data)

    def remove_entry_listener(self, registration_id):
        return self._stop_listening(registration_id,
                                    lambda i: map_remove_entry_listener_codec.encode_request(self.name, i))

    def replace(self, key, value):
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")

        key_data = self._to_data(key)
        value_data = self._to_data(value)

        return self._replace_internal(key_data, value_data)

    def replace_if_same(self, key, old_value, new_value):
        check_not_none(key, "key can't be None")
        check_not_none(old_value, "old_value can't be None")
        check_not_none(new_value, "new_value can't be None")

        key_data = self._to_data(key)
        old_value_data = self._to_data(old_value)
        new_value_data = self._to_data(new_value)

        return self._replace_if_same_internal(key_data, old_value_data, new_value_data)

    def set(self, key, value, ttl=-1):
        """
        :param key:
        :param value:
        :param ttl:
        :return:
        """
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._set_internal(key_data, value_data, ttl)

    def size(self):
        return self._encode_invoke(map_size_codec)

    def try_lock(self, key, ttl=-1, timeout=0):
        check_not_none(key, "key can't be None")

        key_data = self._to_data(key)

        return self._encode_invoke_on_key(map_try_lock_codec, key_data, key=key_data, thread_id=thread_id(), lease=to_millis(ttl),
                                          timeout=to_millis(timeout))

    def try_put(self, key, value, timeout=0):
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")

        key_data = self._to_data(key)
        value_data = self._to_data(value)

        return self._try_put_internal(key_data, value_data, timeout)

    def try_remove(self, key, timeout=0):
        check_not_none(key, "key can't be None")

        key_data = self._to_data(key)
        return self._try_remove_internal(key_data, timeout)

    def unlock(self, key):
        check_not_none(key, "key can't be None")

        key_data = self._to_data(key)

        return self._encode_invoke_on_key(map_unlock_codec, key_data, key=key_data, thread_id=thread_id())

    def values(self, predicate=None):
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
        for partition_id, key_dict in partition_to_keys.iteritems():
            future = self._encode_invoke_on_partition(map_get_all_codec, partition_id, keys=key_dict.values())
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

    def _handle_invalidation(self, key_data):
        # null key means near cache has to remove all entries in it.
        # see MapAddNearCacheEntryListenerMessageTask.
        if key_data is None:
            self._near_cache.clear()
        else:
            del self._near_cache[key_data]

    def _handle_batch_invalidation(self, key_data_list):
        for key_data in key_data_list:
            del self._near_cache[key_data]

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
        for key_dic in partition_to_keys.itervalues():
            for key in key_dic.keys():
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
