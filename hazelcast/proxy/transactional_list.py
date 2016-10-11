from hazelcast.protocol.codec import transactional_list_add_codec, transactional_list_remove_codec, \
    transactional_list_size_codec
from hazelcast.proxy.base import TransactionalProxy
from hazelcast.util import check_not_none


class TransactionalList(TransactionalProxy):
    """
    Transactional implementation of :class:`~hazelcast.proxy.list.List`.
    """
    def add(self, item):
        """
        Transactional implementation of :func:`List.add(item) <hazelcast.proxy.list.List.add>`

        :param item: (object), the new item to be added.
        :return: (bool), ``true`` if the item is added successfully, ``false`` otherwise.
        """
        check_not_none(item, "item can't be none")
        return self._encode_invoke(transactional_list_add_codec, item=self._to_data(item))

    def remove(self, item):
        """
        Transactional implementation of :func:`List.remove(item) <hazelcast.proxy.list.List.remove>`

        :param item: (object), the specified item to be removed.
        :return: (bool), ``true`` if the item is removed successfully, ``false`` otherwise.
        """
        check_not_none(item, "item can't be none")
        return self._encode_invoke(transactional_list_remove_codec, item=self._to_data(item))

    def size(self):
        """
        Transactional implementation of :func:`List.size() <hazelcast.proxy.list.List.size>`

        :return: (int), the size of the list.
        """
        return self._encode_invoke(transactional_list_size_codec)
