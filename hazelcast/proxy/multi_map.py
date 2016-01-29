from hazelcast.protocol.codec import multi_map_add_entry_listener_codec, multi_map_add_entry_listener_to_key_codec, \
    multi_map_clear_codec, multi_map_contains_entry_codec, multi_map_contains_key_codec, multi_map_contains_value_codec, \
    multi_map_entry_set_codec, multi_map_force_unlock_codec, multi_map_get_codec, multi_map_is_locked_codec, \
    multi_map_key_set_codec, multi_map_lock_codec, multi_map_put_codec, multi_map_remove_codec, \
    multi_map_remove_entry_codec, multi_map_remove_entry_listener_codec, multi_map_size_codec, multi_map_try_lock_codec, \
    multi_map_unlock_codec, multi_map_value_count_codec, multi_map_values_codec
from hazelcast.proxy.base import Proxy, EntryEvent, EntryEventType
from hazelcast.util import check_not_none, thread_id, to_millis


class MultiMap(Proxy):
    def add_entry_listener(self, include_value=False, key=None, added=None, removed=None, clear_all=None):
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
            if event.event_type == EntryEventType.added and added:
                added(event)
            elif event.event_type == EntryEventType.removed and removed:
                removed(event)
            elif event.event_type == EntryEventType.clear_all and clear_all:
                clear_all(event)

        return self._start_listening(request,
                                     lambda m: multi_map_add_entry_listener_codec.handle(m,
                                                                                         handle_event_entry),
                                     lambda r: multi_map_add_entry_listener_codec.decode_response(r)[
                                         'response'])

    def contains_key(self, key):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_contains_key_codec, key_data, key=key_data,
                                          thread_id=thread_id())

    def contains_value(self, value):
        check_not_none(value, "value can't be None")
        value_data = self._to_data(value)
        return self._encode_invoke(multi_map_contains_value_codec, value=value_data)

    def contains_entry(self, key, value):
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._encode_invoke_on_key(multi_map_contains_entry_codec, key_data, key=key_data,
                                          value=value_data, thread_id=thread_id())

    def clear(self):
        return self._encode_invoke(multi_map_clear_codec)

    def entry_set(self):
        return self._encode_invoke(multi_map_entry_set_codec)

    def get(self, key):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_get_codec, key_data, key=key_data,
                                          thread_id=thread_id())

    def is_locked(self, key):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_is_locked_codec, key_data, key=key_data)

    def force_unlock(self, key):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_force_unlock_codec, key_data, key=key_data)

    def key_set(self):
        return self._encode_invoke(multi_map_key_set_codec)

    def lock(self, key, lease_time=-1):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_lock_codec, key_data, key=key_data,
                                          thread_id=thread_id(), ttl=to_millis(lease_time))

    def remove(self, key, value):
        check_not_none(key, "key can't be None")
        check_not_none(key, "value can't be None")
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._encode_invoke_on_key(multi_map_remove_entry_codec, key_data, key=key_data,
                                          value=value_data, thread_id=thread_id())

    def remove_all(self, key):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_remove_codec, key_data, key=key_data,
                                          thread_id=thread_id())

    def put(self, key, value):
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._encode_invoke_on_key(multi_map_put_codec, key_data, key=key_data, value=value_data,
                                          thread_id=thread_id())

    def remove_entry_listener(self, registration_id):
        return self._stop_listening(registration_id,
                                    lambda i: multi_map_remove_entry_listener_codec.encode_request(self.name, i))

    def size(self):
        return self._encode_invoke(multi_map_size_codec)

    def value_count(self, key):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_value_count_codec, key_data, key=key_data,
                                          thread_id=thread_id())

    def values(self):
        return self._encode_invoke(multi_map_values_codec)

    def try_lock(self, key, lease_time=-1, timeout=-1):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_try_lock_codec, key_data, key=key_data,
                                          thread_id=thread_id(), lease=to_millis(lease_time),
                                          timeout=to_millis(timeout))

    def unlock(self, key):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(multi_map_unlock_codec, key_data, key=key_data,
                                          thread_id=thread_id())
