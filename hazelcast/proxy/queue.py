from hazelcast.protocol.codec import \
    queue_add_all_codec, \
    queue_add_listener_codec, \
    queue_clear_codec, \
    queue_compare_and_remove_all_codec, \
    queue_compare_and_retain_all_codec, \
    queue_contains_all_codec, \
    queue_contains_codec, \
    queue_drain_to_max_size_codec, \
    queue_is_empty_codec, \
    queue_iterator_codec, \
    queue_offer_codec, \
    queue_peek_codec, \
    queue_poll_codec, \
    queue_put_codec, \
    queue_remaining_capacity_codec, \
    queue_remove_codec, \
    queue_remove_listener_codec, \
    queue_size_codec, \
    queue_take_codec
from hazelcast.proxy.collection import Collection
from hazelcast.util import check_not_none


class Empty(Exception):
    pass


class Full(Exception):
    pass


class Queue(Collection):
    def add(self, item):
        def result_fnc(f):
            if f.result():
                return True
            raise Full("Queue is full!")
        return self.offer(item).continue_with(result_fnc)

    def add_all(self, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke_on_partition(queue_add_all_codec, name=self.name, data_list=data_items)

    def add_listener(self, include_value=False, item_added=None, item_removed=None):
        return self._add_listener(include_value, item_added, item_removed, queue_add_listener_codec)

    def clear(self):
        return self._encode_invoke_on_partition(queue_clear_codec, name=self.name)

    def contains(self, item):
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke_on_partition(queue_contains_codec, name=self.name, value=item_data)

    def contains_all(self, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke_on_partition(queue_contains_all_codec, name=self.name, data_list=data_items)

    def drain_to(self, list, max_size=-1):
        def drain_result(f):
            resp = f.result()
            list.extend(resp)
            return len(resp)
        return self._encode_invoke_on_partition(queue_drain_to_max_size_codec, name=self.name, max_size=max_size).continue_with(
            drain_result)

    def iterator(self):
        return self._encode_invoke_on_partition(queue_iterator_codec, name=self.name)

    def is_empty(self):
        return self._encode_invoke_on_partition(queue_is_empty_codec, name=self.name)

    def offer(self, item, timeout=0):
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        _timeout = timeout * 1000
        return self._encode_invoke_on_partition(queue_offer_codec, name=self.name, value=element_data, timeout_millis=_timeout)

    def peek(self):
        return self._encode_invoke_on_partition(queue_peek_codec, name=self.name)

    def poll(self, timeout=0):
        _timeout = timeout * 1000
        return self._encode_invoke_on_partition(queue_poll_codec, name=self.name, timeout_millis=_timeout)

    def put(self, item):
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_invoke_on_partition(queue_put_codec, name=self.name, value=element_data)

    def remaining_capacity(self):
        return self._encode_invoke_on_partition(queue_remaining_capacity_codec, name=self.name)

    def remove(self, item):
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke_on_partition(queue_remove_codec, name=self.name, value=item_data)

    def remove_all(self, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke_on_partition(queue_compare_and_remove_all_codec, name=self.name, data_list=data_items)

    def remove_listener(self, registration_id):
        return self._stop_listening(registration_id, lambda i: queue_remove_listener_codec.encode_request(self.name, i))

    def retain_all(self, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke_on_partition(queue_compare_and_retain_all_codec, name=self.name, data_list=data_items)

    def size(self):
        return self._encode_invoke_on_partition(queue_size_codec, name=self.name)

    def take(self):
        return self._encode_invoke_on_partition(queue_take_codec, name=self.name)

    def __str__(self):
        return "Queue(name=%s)" % self.name
