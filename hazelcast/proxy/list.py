from hazelcast.protocol.codec import list_add_codec, list_get_codec, list_add_with_index_codec, list_add_all_codec, \
    list_add_all_with_index_codec
from hazelcast.proxy.base import HAS_RESP, VOID_RESP
from hazelcast.proxy.base import PartitionSpecificClientProxy
from hazelcast.util import check_not_none


class List(PartitionSpecificClientProxy):
    def add(self, item):
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_and_invoke(list_add_codec, HAS_RESP, self.name, element_data)

    def add_at(self, index, item):
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_and_invoke(list_add_with_index_codec, VOID_RESP, self.name, index, element_data)

    def add_all(self, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_and_invoke(list_add_all_codec, HAS_RESP, self.name, data_items)

    def add_all_at(self, index, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_and_invoke(list_add_all_with_index_codec, HAS_RESP, self.name, index, data_items)

    def add_listener(self, item_added=None, item_removed=None):
        raise NotImplementedError

    def clear(self):
        raise NotImplementedError

    def contains(self, item):
        raise NotImplementedError

    def contains_all(self, items):
        raise NotImplementedError

    def get(self, index):
        return self._encode_and_invoke(list_get_codec, HAS_RESP, self.name, index)

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

    def _encode_and_invoke(self, codec, has_response, *args):
        request = codec.encode_request(*args)
        return self._invoke_on_partition(request).continue_with(self._handle_response, codec.decode_response, has_response)

    def _handle_response(self, future, decode_function, has_response=False):
        response = future.result()
        if response is not None and has_response:
            params = decode_function(response)
            result_data = params['response']
            return self._to_object(result_data)
        if future.exception():
            raise future.exception()
