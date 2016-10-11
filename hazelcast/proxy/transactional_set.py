from hazelcast.protocol.codec import transactional_set_add_codec, transactional_set_remove_codec, \
    transactional_set_size_codec
from hazelcast.proxy.base import TransactionalProxy
from hazelcast.util import check_not_none


class TransactionalSet(TransactionalProxy):
    """
    Transactional implementation of :class:`~hazelcast.proxy.set.Set`.
    """
    def add(self, item):
        """
        Transactional implementation of :func:`Set.add(item) <hazelcast.proxy.set.Set.add>`

        :param item: (object), the new item to be added.
        :return: (bool), ``true`` if item is added successfully, ``false`` otherwise.
        """
        check_not_none(item, "item can't be none")
        return self._encode_invoke(transactional_set_add_codec, item=self._to_data(item))

    def remove(self, item):
        """
        Transactional implementation of :func:`Set.remove(item) <hazelcast.proxy.set.Set.remove>`

        :param item: (object), the specified item to be deleted.
        :return: (bool), ``true`` if item is remove successfully, ``false`` otherwise.
        """
        check_not_none(item, "item can't be none")
        return self._encode_invoke(transactional_set_remove_codec, item=self._to_data(item))

    def size(self):
        """
        Transactional implementation of :func:`Set.size() <hazelcast.proxy.set.Set.size>`

        :return: (int), size of the set.
        """
        return self._encode_invoke(transactional_set_size_codec)
