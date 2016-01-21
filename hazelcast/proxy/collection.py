from hazelcast.proxy.base import PartitionSpecificClientProxy, ItemEvent, ItemEventType
from hazelcast.util import check_not_none


class Collection(PartitionSpecificClientProxy):
    def _add(self, item, add_codec=None):
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_invoke_on_partition(add_codec, name=self.name, value=element_data)

    def _add_all(self, items, add_all_codec=None):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke_on_partition(add_all_codec, name=self.name, value_list=data_items)

    def _add_listener(self, include_value=False, item_added=None, item_removed=None, add_listener_codec=None):
        request = add_listener_codec.encode_request(self.name, include_value, False)

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
                                     lambda m: add_listener_codec.handle(m, handle_event_item),
                                     lambda r: add_listener_codec.decode_response(r)['response'],
                                     self.get_partition_key())

    def _clear(self, clear_codec=None):
        return self._encode_invoke_on_partition(clear_codec, name=self.name)

    def _contains(self, item, contains_codec=None):
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke_on_partition(contains_codec, name=self.name, value=item_data)

    def _contains_all(self, items, contains_all_codec=None):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke_on_partition(contains_all_codec, name=self.name, value_list=data_items)

    def _get_all(self, get_all_codec=None):
        return self._encode_invoke_on_partition(get_all_codec, name=self.name)

    def _is_empty(self, is_empty_codec=None):
        return self._encode_invoke_on_partition(is_empty_codec, name=self.name)

    def _remove(self, item, remove_codec=None):
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke_on_partition(remove_codec, name=self.name, value=item_data)

    def _remove_all(self, items, compare_and_remove_all_codec=None):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke_on_partition(compare_and_remove_all_codec, name=self.name, values=data_items)

    def _remove_listener(self, registration_id, remove_listener_codec=None):
        return self._stop_listening(registration_id, lambda i: remove_listener_codec.encode_request(self.name, i))

    def _retain_all(self, items, compare_and_retain_all_codec=None):
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke_on_partition(compare_and_retain_all_codec, name=self.name, values=data_items)

    def _size(self, size_codec=None):
        return self._encode_invoke_on_partition(size_codec, name=self.name)

    def __str__(self):
        return "Set(name=%s)" % self.name
