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
from hazelcast.util import check_not_none, ImmutableLazyDataList


class Set(PartitionSpecificProxy):
    """
    Concurrent, distributed implementation of ``Set``.
    """
    def add(self, item):
        """
        Adds the specified item if it is not exists in this set.

        :param item: (object), the specified item to be added.
        :return: (bool), ``true`` if this set is changed after call, ``false`` otherwise.
        """
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        request = set_add_codec.encode_request(self.name, element_data)
        return self._invoke(request, set_add_codec.decode_response)

    def add_all(self, items):
        """
        Adds the elements in the specified collection if they're not exist in this set.

        :param items: (Collection), collection which includes the items to be added.
        :return: (bool), ``true`` if this set is changed after call, ``false`` otherwise.
        """
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))

        request = set_add_all_codec.encode_request(self.name, data_items)
        return self._invoke(request, set_add_all_codec.decode_response)

    def add_listener(self, include_value=False, item_added_func=None, item_removed_func=None):
        """
        Adds an item listener for this container. Listener will be notified for all container add/remove events.

        :param include_value: (bool), whether received events include the updated item or not (optional).
        :param item_added_func: Function to be called when an item is added to this set (optional).
        :param item_removed_func: Function to be called when an item is deleted from this set (optional).
        :return: (str), a registration id which is used as a key to remove the listener.
        """
        request = set_add_listener_codec.encode_request(self.name, include_value, self._is_smart)

        def handle_event_item(item, uuid, event_type):
            item = item if include_value else None
            member = self._client.cluster.get_member(uuid)

            item_event = ItemEvent(self.name, item, event_type, member, self._to_object)
            if event_type == ItemEventType.added:
                if item_added_func:
                    item_added_func(item_event)
            else:
                if item_removed_func:
                    item_removed_func(item_event)

        return self._register_listener(request, lambda r: set_add_listener_codec.decode_response(r),
                                       lambda reg_id: set_remove_listener_codec.encode_request(self.name, reg_id),
                                       lambda m: set_add_listener_codec.handle(m, handle_event_item))

    def clear(self):
        """
        Clears the set. Set will be empty with this call.
        """
        request = set_clear_codec.encode_request(self.name)
        return self._invoke(request)

    def contains(self, item):
        """
        Determines whether this set contains the specified item or not.

        :param item: (object), the specified item to be searched.
        :return: (bool), ``true`` if the specified item exists in this set, ``false`` otherwise.
        """
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        request = set_contains_codec.encode_request(self.name, item_data)
        return self._invoke(request, set_contains_codec.decode_response)

    def contains_all(self, items):
        """
        Determines whether this set contains all of the items in the specified collection or not.

        :param items: (Collection), the specified collection which includes the items to be searched.
        :return: (bool), ``true`` if all of the items in the specified collection exist in this set, ``false`` otherwise.
        """
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))

        request = set_contains_all_codec.encode_request(self.name, data_items)
        return self._invoke(request, set_contains_all_codec.decode_response)

    def get_all(self):
        """
        Returns all of the items in the set.

        :return: (Sequence), list of the items in this set.
        """
        def handler(message):
            return ImmutableLazyDataList(set_get_all_codec.decode_response(message), self._to_object)

        request = set_get_all_codec.encode_request(self.name)
        return self._invoke(request, handler)

    def is_empty(self):
        """
        Determines whether this set is empty or not.

        :return: (bool), ``true`` if this set is empty, ``false`` otherwise.
        """
        request = set_is_empty_codec.encode_request(self.name)
        return self._invoke(request, set_is_empty_codec.decode_response)

    def remove(self, item):
        """
        Removes the specified element from the set if it exists.

        :param item: (object), the specified element to be removed.
        :return: (bool), ``true`` if the specified element exists in this set.
        """
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        request = set_remove_codec.encode_request(self.name, item_data)
        return self._invoke(request, set_remove_codec.decode_response)

    def remove_all(self, items):
        """
        Removes all of the elements of the specified collection from this set.

        :param items: (Collection), the specified collection.
        :return: (bool), ``true`` if the call changed this set, ``false`` otherwise.
        """
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))

        request = set_compare_and_remove_all_codec.encode_request(self.name, data_items)
        return self._invoke(request, set_compare_and_remove_all_codec.decode_response)

    def remove_listener(self, registration_id):
        """
        Removes the specified item listener. Returns silently if the specified listener was not added before.

        :param registration_id: (str), id of the listener to be deleted.
        :return: (bool), ``true`` if the item listener is removed, ``false`` otherwise.
        """
        return self._deregister_listener(registration_id)

    def retain_all(self, items):
        """
        Removes the items which are not contained in the specified collection. In other words, only the items that are
        contained in the specified collection will be retained.

        :param items: (Collection), collection which includes the elements to be retained in this set.
        :return: (bool), ``true`` if this set changed as a result of the call.
        """
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))

        request = set_compare_and_retain_all_codec.encode_request(self.name, data_items)
        return self._invoke(request, set_compare_and_retain_all_codec.decode_response)

    def size(self):
        """
        Returns the number of items in this set.

        :return: (int), number of items in this set.
        """
        request = set_size_codec.encode_request(self.name)
        return self._invoke(request, set_size_codec.decode_response)
