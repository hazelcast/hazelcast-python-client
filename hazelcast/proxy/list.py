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
    """
    Concurrent, distributed implementation of List.

    The Hazelcast List is not a partitioned data-structure. So all the content of the List is stored in a single
    machine (and in the backup). So the List will not scale by adding more members in the cluster.
    """
    def add(self, item):
        """
        Adds the specified item to the end of this list.

        :param item: (object), the specified item to be appended to this list.
        :return: (bool), ``true`` if item is added, ``false`` otherwise.
        """
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_invoke(list_add_codec, value=element_data)

    def add_at(self, index, item):
        """
        Adds the specified item at the specific position in this list. Element in this position and following elements
        are shifted to the right, if any.

        :param index: (int), the specified index to insert the item.
        :param item: (object), the specified item to be inserted.
        """
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_invoke(list_add_with_index_codec, index=index, value=element_data)

    def add_all(self, items):
        """
        Adds all of the items in the specified collection to the end of this list. The order of new elements is
        determined by the specified collection's iterator.

        :param items: (Collection), the specified collection which includes the elements to be added to list.
        :return: (bool), ``true`` if this call changed the list, ``false`` otherwise.
        """
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(list_add_all_codec, value_list=data_items)

    def add_all_at(self, index, items):
        """
        Adds all of the elements in the specified collection into this list at the specified position. Elements in this
        positions and following elements are shifted to the right, if any. The order of new elements is determined by the
        specified collection's iterator.

        :param index: (int), the specified index at which the first element of specified collection is added.
        :param items: (Collection), the specified collection which includes the elements to be added to list.
        :return: (bool), ``true`` if this call changed the list, ``false`` otherwise.
        """
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(list_add_all_with_index_codec, index=index, value_list=data_items)

    def add_listener(self, include_value=False, item_added_func=None, item_removed_func=None):
        """
        Adds an item listener for this list. Listener will be notified for all list add/remove events.

        :param include_value: (bool), whether received events include the updated item or not (optional).
        :param item_added_func: Function to be called when an item is added to this list (optional).
        :param item_removed_func: Function to be called when an item is deleted from this list (optional).
        :return: (str), a registration id which is used as a key to remove the listener.
        """
        request = list_add_listener_codec.encode_request(self.name, include_value, False)

        def handle_event_item(item, uuid, event_type):
            item = item if include_value else None
            member = self._client.cluster.get_member_by_uuid(uuid)

            item_event = ItemEvent(self.name, item, event_type, member, self._to_object)
            if event_type == ItemEventType.added:
                if item_added_func:
                    item_added_func(item_event)
            else:
                if item_removed_func:
                    item_removed_func(item_event)

        return self._start_listening(request,
                                     lambda m: list_add_listener_codec.handle(m, handle_event_item),
                                     lambda r: list_add_listener_codec.decode_response(r)['response'],
                                     self.partition_key)

    def clear(self):
        """
        Clears the list. List will be empty with this call.
        """
        return self._encode_invoke(list_clear_codec)

    def contains(self, item):
        """
        Determines whether this list contains the specified item or not.

        :param item: (object), the specified item.
        :return: (bool), ``true`` if the specified item exists in this list, ``false`` otherwise.
        """
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke(list_contains_codec, value=item_data)

    def contains_all(self, items):
        """
        Determines whether this list contains all of the items in specified collection or not.

        :param items: (Collection), the specified collection which includes the items to be searched.
        :return: (bool), ``true`` if all of the items in specified collection exist in this list, ``false`` otherwise.
        """
        check_not_none(items, "Items can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "item can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(list_contains_all_codec, values=data_items)

    def get(self, index):
        """
        Returns the item which is in the specified position in this list.

        :param index: (int), the specified index of the item to be returned.
        :return: (object), the item in the specified position in this list.
        """
        return self._encode_invoke(list_get_codec, index=index)

    def get_all(self):
        """
        Returns all of the items in this list.

        :return: (Sequence), list that includes all of the items in this list.
        """
        return self._encode_invoke(list_get_all_codec)

    def iterator(self):
        """
        Returns an iterator over the elements in this list in proper sequence, same with get_all().

        :return: (Sequence), an iterator over the elements in this list in proper sequence.
        """
        return self._encode_invoke(list_iterator_codec)

    def index_of(self, item):
        """
        Returns the first index of specified items's occurrences in this list. If specified item is not present in this
        list, returns -1.

        :param item: (object), the specified item to be searched for.
        :return: (int), the first index of specified items's occurrences, -1 if item is not present in this list.
        """
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke(list_index_of_codec, value=item_data)

    def is_empty(self):
        """
        Determines whether this list is empty or not.

        :return: (bool), ``true`` if this list contains no elements.
        """
        return self._encode_invoke(list_is_empty_codec)

    def last_index_of(self, item):
        """
        Returns the last index of specified items's occurrences in this list. If specified item is not present in this
        list, returns -1.

        :param item: (object), the specified item to be searched for.
        :return: (int), the last index of specified items's occurrences, -1 if item is not present in this list.
        """
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke(list_last_index_of_codec, value=item_data)

    def list_iterator(self, index=0):
        """
        Returns a list iterator of the elements in this list. If an index is provided, iterator starts from this index.

        :param index: (int), index of first element to be returned from the list iterator (optional).
        :return: (Sequence), a list iterator of the elements in this list.
        """
        return self._encode_invoke(list_list_iterator_codec, index=index)

    def remove(self, item):
        """
        Removes the specified element's first occurrence from the list if it exists in this list.

        :param item: (object), the specified element.
        :return: (bool), ``true`` if the specified element is present in this list.
        """
        check_not_none(item, "Value can't be None")
        item_data = self._to_data(item)
        return self._encode_invoke(list_remove_codec, value=item_data)

    def remove_at(self, index):
        """
        Removes the item at the specified position in this list. Element in this position and following elements are
        shifted to the left, if any.

        :param index: (int), index of the item to be removed.
        :return: (object), the item previously at the specified index.
        """
        return self._encode_invoke(list_remove_with_index_codec, index=index)

    def remove_all(self, items):
        """
        Removes all of the elements that is present in the specified collection from this list.

        :param items: (Collection), the specified collection.
        :return: (bool), ``true`` if this list changed as a result of the call.
        """
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(list_compare_and_remove_all_codec, values=data_items)

    def remove_listener(self, registration_id):
        """
        Removes the specified item listener. Returns silently if the specified listener was not added before.

        :param registration_id: (str), id of the listener to be deleted.
        :return: (bool), ``true`` if the item listener is removed, ``false`` otherwise.
        """
        return self._stop_listening(registration_id, lambda i: list_remove_listener_codec.encode_request(self.name, i))

    def retain_all(self, items):
        """
        Retains only the items that are contained in the specified collection. It means, items which are not present in
        the specified collection are removed from this list.

        :param items: (Collection), collections which includes the elements to be retained in this list.
        :return: (bool), ``true`` if this list changed as a result of the call.
        """
        check_not_none(items, "Value can't be None")
        data_items = []
        for item in items:
            check_not_none(item, "Value can't be None")
            data_items.append(self._to_data(item))
        return self._encode_invoke(list_compare_and_retain_all_codec, values=data_items)

    def size(self):
        """
        Returns the number of elements in this list.

        :return: (int), number of the elements in this list.
        """
        return self._encode_invoke(list_size_codec)

    def set_at(self, index, item):
        """
        Replaces the specified element with the element at the specified position in this list.

        :param index: (int), index of the item to be replaced.
        :param item: (object), item to be stored.
        :return: (object), the previous item in the specified index.
        """
        check_not_none(item, "Value can't be None")
        element_data = self._to_data(item)
        return self._encode_invoke(list_set_codec, index=index, value=element_data)

    def sub_list(self, from_index, to_index):
        """
        Returns a sublist from this list, whose range is specified with from_index(inclusive) and to_index(exclusive).
        The returned list is backed by this list, so non-structural changes in the returned list are reflected in this
        list, and vice-versa.

        :param from_index: (int), the start point(inclusive) of the sub_list.
        :param to_index: (int), th end point(exclusive) of the sub_list.
        :return: (Sequence), a view of the specified range within this list.
        """
        return self._encode_invoke(list_sub_codec, from_=from_index, to=to_index)
