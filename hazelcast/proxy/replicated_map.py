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


class ReplicatedMap(Proxy):
    _partition_id = None

    def add_entry_listener(self, key=None, predicate=None, added=None, removed=None, updated=None,
                           evicted=None, clear_all=None):
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
            if event.event_type == EntryEventType.added and added:
                added(event)
            elif event.event_type == EntryEventType.removed and removed:
                removed(event)
            elif event.event_type == EntryEventType.updated and updated:
                updated(event)
            elif event.event_type == EntryEventType.evicted and evicted:
                evicted(event)
            elif event.event_type == EntryEventType.clear_all and clear_all:
                clear_all(event)

        return self._start_listening(request,
                                     lambda m: replicated_map_add_entry_listener_codec.handle(m,
                                                                                              handle_event_entry),
                                     lambda r: replicated_map_add_entry_listener_codec.decode_response(r)[
                                         'response'])

    def clear(self):
        return self._encode_invoke(replicated_map_clear_codec)

    def contains_key(self, key):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(replicated_map_contains_key_codec, key_data, key=key_data)

    def contains_value(self, value):
        check_not_none(value, "value can't be None")
        return self._encode_invoke_on_target_partition(replicated_map_contains_value_codec, value=self._to_data(value))

    def entry_set(self):
        return self._encode_invoke_on_target_partition(replicated_map_entry_set_codec)

    def get(self, key):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(replicated_map_get_codec, key_data, key=key_data)

    def is_empty(self):
        return self._encode_invoke_on_target_partition(replicated_map_is_empty_codec)

    def key_set(self):
        return self._encode_invoke_on_target_partition(replicated_map_key_set_codec)

    def put(self, key, value, ttl=0):
        check_not_none(key, "key can't be None")
        check_not_none(key, "value can't be None")
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        return self._encode_invoke_on_key(replicated_map_put_codec, key_data, key=key_data, value=value_data,
                                          ttl=to_millis(ttl))

    def put_all(self, map):
        entries = {}
        for key, value in map.iteritems():
            check_not_none(key, "key can't be None")
            check_not_none(value, "value can't be None")
            entries[self._to_data(key)] = self._to_data(value)
        self._encode_invoke(replicated_map_put_all_codec, entries=entries)

    def remove(self, key):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(replicated_map_remove_codec, key_data, key=key_data)

    def remove_entry_listener(self, registration_id):
        return self._stop_listening(registration_id,
                                    lambda i: replicated_map_remove_entry_listener_codec.encode_request(self.name, i))

    def size(self):
        return self._encode_invoke_on_target_partition(replicated_map_size_codec)

    def values(self):
        return self._encode_invoke_on_target_partition(replicated_map_values_codec)

    def _get_partition_id(self):
        if not self._partition_id:
            self._partition_id = randint(0, self._client.partition_service.get_partition_count() - 1)
        return self._partition_id

    def _encode_invoke_on_target_partition(self, codec, response_handler=default_response_handler, **kwargs):
        return self._encode_invoke_on_partition(codec, self._get_partition_id(), response_handler, **kwargs)
