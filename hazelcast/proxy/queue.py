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
from hazelcast.proxy.base import PartitionSpecificProxy, ItemEvent, ItemEventType
from hazelcast.util import check_not_none, to_millis


class Empty(Exception):
    pass


class Full(Exception):
    pass


class Queue(PartitionSpecificProxy):
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
        return self._encode_invoke(queue_add_all_codec, data_list=data_items)

    def add_listener(self, include_value=False, item_added=None, item_removed=None):
        request = queue_add_listener_codec.encode_request(self.name, include_value, False)

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
                                     lambda m: queue_add_listener_codec.handle(m, handle_event_item),
                                     lambda r: queue_add_listener_codec.decode_response(r)['response'],
                                     self.partition_key)

    def clear(self):
        return self._encode_invoke(queue_clear_codec)

    def contains(self, item):
        check_not_none(item, "Item can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke(queue_contains_codec, value=item_data)

    def contains_all(self, items):
        check_not_none(items, "Items can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "item can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(queue_contains_all_codec, data_list=data_items)

    def drain_to(self, list, max_size=-1):
        def drain_result(f):
            resp = f.result()
            list.extend(resp)
            return len(resp)

        return self._encode_invoke(queue_drain_to_max_size_codec, max_size=max_size).continue_with(
            drain_result)

    def iterator(self):
        return self._encode_invoke(queue_iterator_codec)

    def is_empty(self):
        return self._encode_invoke(queue_is_empty_codec)

    def offer(self, item, timeout=0):
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_invoke(queue_offer_codec, value=element_data, timeout_millis=to_millis(timeout))

    def peek(self):
        return self._encode_invoke(queue_peek_codec)

    def poll(self, timeout=0):
        return self._encode_invoke(queue_poll_codec, timeout_millis=to_millis(timeout))

    def put(self, item):
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_invoke(queue_put_codec, value=element_data)

    def remaining_capacity(self):
        return self._encode_invoke(queue_remaining_capacity_codec)

    def remove(self, item):
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke(queue_remove_codec, value=item_data)

    def remove_all(self, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(queue_compare_and_remove_all_codec, data_list=data_items)

    def remove_listener(self, registration_id):
        return self._stop_listening(registration_id, lambda i: queue_remove_listener_codec.encode_request(self.name, i))

    def retain_all(self, items):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(queue_compare_and_retain_all_codec, data_list=data_items)

    def size(self):
        return self._encode_invoke(queue_size_codec)

    def take(self):
        return self._encode_invoke(queue_take_codec)
