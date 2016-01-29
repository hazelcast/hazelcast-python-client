from hazelcast.protocol.codec import list_add_all_codec, \
    list_add_all_with_index_codec, \
    list_add_codec, \
    list_add_listener_codec, \
    list_add_with_index_codec, \
    list_clear_codec, \
    list_compare_and_remove_all_codec, \
    list_compare_and_retain_all_codec, \
    list_contains_all_codec, \
    list_contains_codec, \
    list_get_all_codec, \
    list_get_codec, \
    list_index_of_codec, \
    list_is_empty_codec, \
    list_iterator_codec, \
    list_last_index_of_codec, \
    list_list_iterator_codec, \
    list_remove_codec, \
    list_remove_listener_codec, \
    list_remove_with_index_codec, \
    list_set_codec, \
    list_size_codec, \
    list_sub_codec
from hazelcast.proxy.base import PartitionSpecificProxy, ItemEvent, ItemEventType
from hazelcast.util import check_not_none


class List(PartitionSpecificProxy):
    def add(self, item):
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_invoke(list_add_codec, value=element_data)

    def add_at(self, index, item):
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_invoke(list_add_with_index_codec, index=index, value=element_data)

    def add_all(self, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(list_add_all_codec, value_list=data_items)

    def add_all_at(self, index, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(list_add_all_with_index_codec, index=index, value_list=data_items)

    def add_listener(self, include_value=False, item_added=None, item_removed=None):
        request = list_add_listener_codec.encode_request(self.name, include_value, False)

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
                                     lambda m: list_add_listener_codec.handle(m, handle_event_item),
                                     lambda r: list_add_listener_codec.decode_response(r)['response'],
                                     self.partition_key)

    def clear(self):
        return self._encode_invoke(list_clear_codec)

    def contains(self, item):
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke(list_contains_codec, value=item_data)

    def contains_all(self, items):
        check_not_none(items, "Items can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "item can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(list_contains_all_codec, values=data_items)

    def get(self, index):
        return self._encode_invoke(list_get_codec, index=index)

    def get_all(self):
        return self._encode_invoke(list_get_all_codec)

    def iterator(self):
        return self._encode_invoke(list_iterator_codec)

    def index_of(self, item):
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke(list_index_of_codec, value=item_data)

    def is_empty(self):
        return self._encode_invoke(list_is_empty_codec)

    def last_index_of(self, item):
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke(list_last_index_of_codec, value=item_data)

    def list_iterator(self, index=0):
        return self._encode_invoke(list_list_iterator_codec, index=index)

    def remove(self, item):
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke(list_remove_codec, value=item_data)

    def remove_at(self, index):
        return self._encode_invoke(list_remove_with_index_codec, index=index)

    def remove_all(self, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(list_compare_and_remove_all_codec, values=data_items)

    def remove_listener(self, registration_id):
        return self._stop_listening(registration_id, lambda i: list_remove_listener_codec.encode_request(self.name, i))

    def retain_all(self, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(list_compare_and_retain_all_codec, values=data_items)

    def size(self):
        return self._encode_invoke(list_size_codec)

    def set_at(self, index, item):
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_invoke(list_set_codec, index=index, value=element_data)

    def sub_list(self, from_index, to_index):
        return self._encode_invoke(list_sub_codec, from_=from_index, to=to_index)
