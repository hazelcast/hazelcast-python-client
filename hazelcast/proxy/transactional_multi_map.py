from hazelcast.protocol.codec import transactional_multi_map_get_codec, transactional_multi_map_put_codec, \
    transactional_multi_map_remove_codec, transactional_multi_map_remove_entry_codec, \
    transactional_multi_map_size_codec, transactional_multi_map_value_count_codec
from hazelcast.proxy.base import TransactionalProxy
from hazelcast.util import check_not_none


class TransactionalMultiMap(TransactionalProxy):
    """
    Transactional implementation of :class:`~hazelcast.proxy.multi_map.MultiMap`.
    """
    def put(self, key, value):
        """
        Transactional implementation of :func:`MultiMap.put(key, value) <hazelcast.proxy.multi_map.MultiMap.put>`

        :param key: (object), the key to be stored.
        :param value: (object), the value to be stored.
        :return: (bool), ``true`` if the size of the multimap is increased, ``false`` if the multimap already contains the
        key-value tuple.
        """
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        return self._encode_invoke(transactional_multi_map_put_codec, key=self._to_data(key),
                                   value=self._to_data(value))

    def get(self, key):
        """
        Transactional implementation of :func:`MultiMap.get(key) <hazelcast.proxy.multi_map.MultiMap.get>`

        :param key: (object), the key whose associated values are returned.
        :return: (Sequence), the collection of the values associated with the key.
        """
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_multi_map_get_codec, key=self._to_data(key))

    def remove(self, key, value):
        """
        Transactional implementation of :func:`MultiMap.remove(key, value)
        <hazelcast.proxy.multi_map.MultiMap.remove>`

        :param key: (object), the key of the entry to remove.
        :param value: (object), the value of the entry to remove.
        :return:
        """
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        return self._encode_invoke(transactional_multi_map_remove_entry_codec, key=self._to_data(key),
                                   value=self._to_data(value))

    def remove_all(self, key):
        """
        Transactional implementation of :func:`MultiMap.remove_all(key)
        <hazelcast.proxy.multi_map.MultiMap.remove_all>`

        :param key: (object), the key of the entries to remove.
        :return: (list), the collection of the values associated with the key.
        """
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_multi_map_remove_codec, key=self._to_data(key))

    def value_count(self, key):
        """
        Transactional implementation of :func:`MultiMap.value_count(key)
        <hazelcast.proxy.multi_map.MultiMap.value_count>`

        :param key: (object), the key whose number of values is to be returned.
        :return: (int), the number of values matching the given key in the multimap.
        """
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_multi_map_value_count_codec, key=self._to_data(key))

    def size(self):
        """
        Transactional implementation of :func:`MultiMap.size() <hazelcast.proxy.multi_map.MultiMap.size>`

        :return: (int), the number of key-value tuples in the multimap.
        """
        return self._encode_invoke(transactional_multi_map_size_codec)
