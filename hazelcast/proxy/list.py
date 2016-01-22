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

from hazelcast.proxy.collection import Collection
from hazelcast.util import check_not_none


class List(Collection):
    def add(self, item):
        return self._add(item, list_add_codec)

    def add_at(self, index, item):
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_invoke_on_partition(list_add_with_index_codec, name=self.name, index=index, value=element_data)

    def add_all(self, items):
        return self._add_all(items, list_add_all_codec)

    def add_all_at(self, index, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke_on_partition(list_add_all_with_index_codec, name=self.name, index=index,
                                                value_list=data_items)

    def add_listener(self, include_value=False, item_added=None, item_removed=None):
        return self._add_listener(include_value, item_added, item_removed, list_add_listener_codec)

    def clear(self):
        return self._clear(list_clear_codec)

    def contains(self, item):
        return self._contains(item, list_contains_codec)

    def contains_all(self, items):
        check_not_none(items, "Items can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "item can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke_on_partition(list_contains_all_codec, name=self.name, values=data_items)

    def get(self, index):
        return self._encode_invoke_on_partition(list_get_codec, name=self.name, index=index)

    def get_all(self):
        return self._get_all(list_get_all_codec)

    def iterator(self):
        return self._encode_invoke_on_partition(list_iterator_codec, name=self.name)

    def index_of(self, item):
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke_on_partition(list_index_of_codec, name=self.name, value=item_data)

    def is_empty(self):
        return self._is_empty(list_is_empty_codec)

    def last_index_of(self, item):
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke_on_partition(list_last_index_of_codec, name=self.name, value=item_data)

    def list_iterator(self, index=0):
        return self._encode_invoke_on_partition(list_list_iterator_codec, name=self.name, index=index)

    def remove(self, item):
        return self._remove(item, list_remove_codec)

    def remove_at(self, index):
        return self._encode_invoke_on_partition(list_remove_with_index_codec, name=self.name, index=index)

    def remove_all(self, items):
        return self._remove_all(items, list_compare_and_remove_all_codec)

    def remove_listener(self, registration_id):
        return self._remove_listener(registration_id, list_remove_listener_codec)

    def retain_all(self, items):
        return self._retain_all(items, list_compare_and_retain_all_codec)

    def size(self):
        return self._size(list_size_codec)

    def set_at(self, index, item):
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_invoke_on_partition(list_set_codec, name=self.name, index=index, value=element_data)

    def sub_list(self, from_index, to_index):
        return self._encode_invoke_on_partition(list_sub_codec, name=self.name, from_=from_index, to=to_index)

    def __str__(self):
        return "List(name=%s)" % self.name
