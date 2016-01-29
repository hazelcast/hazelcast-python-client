from hazelcast.protocol.codec import \
    set_add_all_codec, \
    set_add_codec, \
    set_add_listener_codec, \
    set_clear_codec, \
    set_compare_and_remove_all_codec, \
    set_compare_and_retain_all_codec, \
    set_contains_all_codec, \
    set_contains_codec, \
    set_get_all_codec, \
    set_is_empty_codec, \
    set_remove_codec, \
    set_remove_listener_codec, \
    set_size_codec

from hazelcast.proxy.base import PartitionSpecificProxy, ItemEvent, ItemEventType
from hazelcast.util import check_not_none


class Set(PartitionSpecificProxy):
    def add(self, item):
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_invoke(set_add_codec, value=element_data)

    def add_all(self, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(set_add_all_codec, value_list=data_items)

    def add_listener(self, include_value=False, item_added=None, item_removed=None):
        request = set_add_listener_codec.encode_request(self.name, include_value, False)

        def handle_event_item(item, uuid, event_type):
            item = item if include_value else None
            member = self._client.cluster.get_member_by_uuid(uuid)

            item_event = ItemEvent(self.name, item, event_type, member, self._to_object)
            if event_type == ItemEventType.added:
                if item_added:
                    item_added(item_event)
            else:
                if item_removed:
                    item_removed(item_event)

        return self._start_listening(request,
                                     lambda m: set_add_listener_codec.handle(m, handle_event_item),
                                     lambda r: set_add_listener_codec.decode_response(r)['response'],
                                     self.partition_key)

    def clear(self):
        return self._encode_invoke(set_clear_codec)

    def contains(self, item):
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke(set_contains_codec, value=item_data)

    def contains_all(self, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(set_contains_all_codec, items=data_items)

    def get_all(self):
        return self._encode_invoke(set_get_all_codec)

    def is_empty(self):
        return self._encode_invoke(set_is_empty_codec)

    def remove(self, item):
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke(set_remove_codec, value=item_data)

    def remove_all(self, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(set_compare_and_remove_all_codec, values=data_items)

    def remove_listener(self, registration_id):
        return self._stop_listening(registration_id, lambda i: set_remove_listener_codec.encode_request(self.name, i))

    def retain_all(self, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(set_compare_and_retain_all_codec, values=data_items)

    def size(self):
        return self._encode_invoke(set_size_codec)