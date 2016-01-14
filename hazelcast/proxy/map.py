from collections import namedtuple
from hazelcast.protocol.codec import map_add_entry_listener_codec, map_contains_key_codec, map_get_codec, map_put_codec, \
    map_size_codec, map_remove_codec, map_remove_entry_listener_codec
from hazelcast.proxy.base import Proxy
from hazelcast.util import check_not_none, enum, thread_id

EntryEventType = enum(added=1,
                      removed=1 << 1,
                      updated=1 << 2,
                      evicted=1 << 3,
                      evict_all=1 << 4,
                      clear_all=1 << 5,
                      merged=1 << 6,
                      expired=1 << 7)

EntryEvent = namedtuple("EntryEvent",
                        ["key", "value", "old_value", "merging_value", "event_type", "uuid",
                         "number_of_affected_entries"])


class Map(Proxy):
    def add_entry_listener(self, include_value=False, key=None, predicate=None, **kwargs):
        flags = self._get_listener_flags(**kwargs)
        request = map_add_entry_listener_codec.encode_request(self.name, include_value, flags, False)

        def handle_event_entry(**_kwargs):
            event = EntryEvent(**_kwargs)
            event_name = EntryEventType.reverse[event.event_type]
            kwargs[event_name](event)

        registration_id = self._start_listening(request,
                                                lambda m: map_add_entry_listener_codec.handle(m, handle_event_entry),
                                                lambda r: map_add_entry_listener_codec.decode_response(r)['response'])
        return registration_id

    def add_index(self, attribute, ordered=False):
        raise NotImplementedError

    def add_interceptor(self, interceptor):
        raise NotImplementedError

    def clear(self):
        raise NotImplementedError

    def contains_key(self, key):
        """
        :param key:
        :return:
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(map_contains_key_codec, key_data,
                                          self.name, key_data, thread_id())

    def contains_value(self, value):
        raise NotImplementedError

    def delete(self, key):
        raise NotImplementedError

    def entry_set(self, predicate=None):
        raise NotImplementedError

    def evict(self, key):
        raise NotImplementedError

    def evict_all(self):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError

    def force_unlock(self, key):
        raise NotImplementedError

    def get(self, key):
        """
        :param key:
        :return:
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(map_get_codec, key_data, self.name, key_data, thread_id())

    def get_all(self, keys):
        raise NotImplementedError

    def get_entry_view(self, key):
        raise NotImplementedError

    def is_empty(self):
        raise NotImplementedError

    def is_locked(self, key):
        raise NotImplementedError

    def key_set(self, predicate=None):
        raise NotImplementedError

    def load_all(self, keys=None, replace_existing_values=True):
        raise NotImplementedError

    def lock(self, key, ttl=-1):
        raise NotImplementedError

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
        return self._encode_invoke_on_key(map_put_codec, key_data, self.name, key_data, value_data, thread_id(), ttl)

    def put_all(self, map):
        raise NotImplementedError

    def put_if_absent(self, key, value):
        raise NotImplementedError

    def put_transient(self, key, value, ttl=-1):
        raise NotImplementedError

    def remove(self, key):
        key_data = self._to_data(key)
        return self._encode_invoke_on_key(map_remove_codec, key_data, self.name, key_data, thread_id())

    def remove_if_same(self, key, value):
        raise NotImplementedError

    def remove_entry_listener(self, registration_id):
        return self._stop_listening(registration_id,
                                    lambda i: map_remove_entry_listener_codec.encode_request(self.name, i))

    def replace(self, key, new_value):
        raise NotImplementedError

    def replace_if_same(self, key, old_value, new_value):
        raise NotImplementedError

    def size(self):
        return self._encode_invoke(map_size_codec, self.name)

    def try_lock(self, key, timeout=-1):
        raise NotImplementedError

    def try_put(self, key, value, timeout=-1):
        raise NotImplementedError

    def try_remove(self, key, timeout=-1):
        raise NotImplementedError

    def unlock(self, key):
        raise NotImplementedError

    def values(self, predicate=None):
        raise NotImplementedError

    @staticmethod
    def _get_listener_flags(**kwargs):
        flags = 0
        for (key, value) in kwargs.iteritems():
            if value is not None:
                flags |= getattr(EntryEventType, key)
        return flags

    def __str__(self):
        return "Map(name=%s)" % self.name
