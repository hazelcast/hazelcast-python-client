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
        if self.offer(item):
            return True
        raise Full("Queue is full!")

    def add_all(self, items):
        return self._add_all(items, queue_add_all_codec)

    def add_listener(self, include_value=False, item_added=None, item_removed=None):
        return self._add_listener(include_value, item_added, item_removed, queue_add_listener_codec)

    def clear(self):
        return self._clear(queue_clear_codec)

    def contains(self, item):
        return self._contains(item, queue_contains_codec)

    def contains_all(self, items):
        return self._contains_all(items, queue_contains_all_codec)

    def drain_to(self, list, max_size=-1):
        self._encode_invoke_on_partition(queue_drain_to_max_size_codec, name=self.name, max_size=max_size)

    def get_all(self):
        return self._get_all(queue_iterator_codec)

    def is_empty(self):
        return self._is_empty(queue_is_empty_codec)

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
        return self._remove(item, queue_remove_codec)

    def remove_all(self, items):
        return self._remove_all(items, queue_compare_and_remove_all_codec)

    def remove_listener(self, registration_id):
        return self._remove_listener(registration_id, queue_remove_listener_codec)

    def retain_all(self, items):
        return self._retain_all(items, queue_compare_and_retain_all_codec)

    def size(self):
        return self._size(queue_size_codec)

    def take(self):
        return self._encode_invoke_on_partition(queue_take_codec, name=self.name)

    def __str__(self):
        return "Queue(name=%s)" % self.name
