from collections import namedtuple
from hazelcast.core import enum
from hazelcast.protocol.codec import map_add_entry_listener_codec, map_contains_key_codec, map_get_codec, map_put_codec, \
    map_size_codec, map_remove_codec
from hazelcast.proxy.base import Proxy, check_not_none, thread_id

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

class MapProxy(Proxy):
    def contains_key(self, key):
        """

        :param key:
        :return:
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)

        request = map_contains_key_codec.encode_request(self.name, key_data, thread_id=thread_id())
        response = self._invoke_on_key(request, key_data)
        return map_contains_key_codec.decode_response(response)['response']

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

        request = map_put_codec.encode_request(self.name, key_data, value_data, thread_id=thread_id(), ttl=ttl)
        response = self._invoke_on_key(request, key_data)
        result_data = map_put_codec.decode_response(response)['response']
        return self._to_object(result_data)

    def get(self, key):
        """
        :param key:
        :return:
        """
        check_not_none(key, "key can't be None")

        key_data = self._to_data(key)
        request = map_get_codec.encode_request(self.name, key_data, thread_id=thread_id())
        response = self._invoke_on_key(request, key_data)
        result_data = map_get_codec.decode_response(response)['response']
        return self._to_object(result_data)

    def remove(self, key):
        key_data = self._to_data(key)
        request = map_remove_codec.encode_request(self.name, key_data, thread_id())
        response = self._invoke_on_key(request, key_data)
        result_data = map_remove_codec.decode_response(response)['response']
        return self._to_object(result_data)

    def size(self):
        request = map_size_codec.encode_request(self.name)
        response = self._invoke(request)
        return map_size_codec.decode_response(response)["response"]

    def add_entry_listener(self, include_value=False, added=None, clear_all=None, evicted=None,
                           evict_all=None, expired=None, merged=None, removed=None,
                           updated=None):
        flags = self._get_listener_flags(added=added,
                                         clear_all=clear_all,
                                         evicted=evicted,
                                         evict_all=evict_all,
                                         expired=expired,
                                         merged=merged,
                                         removed=removed,
                                         updated=updated)
        request = map_add_entry_listener_codec.encode_request(self.name, include_value, flags, False)

        handler_list = locals()

        def handle_event_entry(key, value, old_value, merging_value, event_type, uuid, number_of_affected_entries):
            # TODO: this should be called directly with kwargs
            event = EntryEvent(key=self._to_object(key), value=self._to_object(value),
                               old_value=self._to_object(old_value), merging_value=self._to_object(merging_value),
                               event_type=event_type, uuid=uuid, number_of_affected_entries=number_of_affected_entries)

            event_name = EntryEventType.reverse[event_type]
            handler_list[event_name](event)

        response = self._start_listening(request, lambda m: map_add_entry_listener_codec.handle(m, handle_event_entry))
        return map_add_entry_listener_codec.decode_response(response)['response']

    @staticmethod
    def _get_listener_flags(**kwargs):
        flags = 0
        for (key, value) in kwargs.iteritems():
            if value is not None:
                flags |= getattr(EntryEventType, key)
        return flags

    def __str__(self):
        return "Map(name=%s)" % self.name
