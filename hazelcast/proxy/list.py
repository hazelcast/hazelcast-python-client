from hazelcast.protocol.codec import list_add_codec, list_get_codec, list_add_with_index_codec, list_add_all_codec, \
    list_add_all_with_index_codec
from hazelcast.proxy.base import PartitionSpecificClientProxy
from hazelcast.util import check_not_none


class List(PartitionSpecificClientProxy):
    def add(self, item):
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_invoke_on_partition(list_add_codec, self.name, element_data)

    def add_at(self, index, item):
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_invoke_on_partition(list_add_with_index_codec, self.name, index, element_data)

    def add_all(self, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke_on_partition(list_add_all_codec, self.name, data_items)

    def add_all_at(self, index, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke_on_partition(list_add_all_with_index_codec, self.name, index, data_items)

    def add_listener(self, item_added=None, item_removed=None):
        raise NotImplementedError

    def clear(self):
        raise NotImplementedError

    def contains(self, item):
        raise NotImplementedError

    def contains_all(self, items):
        raise NotImplementedError

    def get(self, index):
        return self._encode_invoke_on_partition(list_get_codec, self.name, index)

    def get_all(self):
        raise NotImplementedError

    def index_of(self, item):
        raise NotImplementedError

    def is_empty(self):
        raise NotImplementedError

    def last_index_of(self, item):
        raise NotImplementedError

    def remove(self, item):
        raise NotImplementedError

    def remove_at(self, index):
        raise NotImplementedError

    def remove_listener(self, registration_id):
        raise NotImplementedError

    def remove_all(self, items):
        raise NotImplementedError

    def retain_all(self, items):
        raise NotImplementedError

    def size(self):
        raise NotImplementedError

    def set_at(self, index, item):
        raise NotImplementedError

    def sub_list(self, from_index, to_index):
        raise NotImplementedError

    def __str__(self):
        return "List(name=%s)" % self.name
