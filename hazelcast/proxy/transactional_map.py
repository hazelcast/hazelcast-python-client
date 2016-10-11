from hazelcast.protocol.codec import transactional_map_contains_key_codec, transactional_map_delete_codec, \
    transactional_map_get_codec, transactional_map_get_for_update_codec, transactional_map_is_empty_codec, \
    transactional_map_key_set_codec, transactional_map_key_set_with_predicate_codec, transactional_map_put_codec, \
    transactional_map_put_if_absent_codec, transactional_map_remove_codec, transactional_map_remove_if_same_codec, \
    transactional_map_replace_codec, transactional_map_replace_if_same_codec, transactional_map_set_codec, \
    transactional_map_size_codec, transactional_map_values_codec, transactional_map_values_with_predicate_codec
from hazelcast.proxy.base import TransactionalProxy
from hazelcast.util import check_not_none, to_millis


class TransactionalMap(TransactionalProxy):
    """
    Transactional implementation of :class:`~hazelcast.proxy.map.Map`.
    """
    def contains_key(self, key):
        """
        Transactional implementation of :func:`Map.contains_key(key) <hazelcast.proxy.map.Map.contains_key>`

        :param key: (object), the specified key.
        :return: (bool), ``true`` if this map contains an entry for the specified key, ``false`` otherwise.
        """
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_map_contains_key_codec, key=self._to_data(key))

    def get(self, key):
        """
        Transactional implementation of :func:`Map.get(key) <hazelcast.proxy.map.Map.get>`

        :param key: (object), the specified key.
        :return: (object), the value for the specified key.
        """
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_map_get_codec, key=self._to_data(key))

    def get_for_update(self, key):
        """
        Locks the key and then gets and returns the value to which the specified key is mapped. Lock will be released at
        the end of the transaction (either commit or rollback).

        :param key: (object), the specified key.
        :return: (object), the value for the specified key.

        .. seealso::
            :func:`Map.get(key) <hazelcast.proxy.map.Map.get>`
        """
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_map_get_for_update_codec, key=self._to_data(key))

    def size(self):
        """
        Transactional implementation of :func:`Map.size() <hazelcast.proxy.map.Map.size>`

        :return: (int), number of entries in this map.
        """
        return self._encode_invoke(transactional_map_size_codec)

    def is_empty(self):
        """
        Transactional implementation of :func:`Map.is_empty() <hazelcast.proxy.map.Map.is_empty>`

        :return: (bool), ``true`` if this map contains no key-value mappings, ``false`` otherwise.
        """

        return self._encode_invoke(transactional_map_is_empty_codec)

    def put(self, key, value, ttl=-1):
        """
        Transactional implementation of :func:`Map.put(key, value, ttl) <hazelcast.proxy.map.Map.put>`

        The object to be put will be accessible only in the current transaction context till the transaction is
        committed.

        :param key: (object), the specified key.
        :param value: (object), the value to associate with the key.
        :param ttl: (int), maximum time in seconds for this entry to stay (optional).
        :return: (object), previous value associated with key or ``None`` if there was no mapping for key.
        """
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        return self._encode_invoke(transactional_map_put_codec, key=self._to_data(key),
                                   value=self._to_data(value), ttl=to_millis(ttl))

    def put_if_absent(self, key, value):
        """
        Transactional implementation of :func:`Map.put_if_absent(key, value)
        <hazelcast.proxy.map.Map.put_if_absent>`

        The object to be put will be accessible only in the current transaction context till the transaction is
        committed.

        :param key: (object), key of the entry.
        :param value: (object), value of the entry.
        :param ttl:  (int), maximum time in seconds for this entry to stay in the map (optional).
        :return: (object), old value of the entry.
        """
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        return self._encode_invoke(transactional_map_put_if_absent_codec, key=self._to_data(key),
                                   value=self._to_data(value))

    def set(self, key, value):
        """
        Transactional implementation of :func:`Map.set(key, value) <hazelcast.proxy.map.Map.set>`

        The object to be set will be accessible only in the current transaction context till the transaction is
        committed.

        :param key: (object), key of the entry.
        :param value: (object), value of the entry.
        """
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        return self._encode_invoke(transactional_map_set_codec, key=self._to_data(key),
                                   value=self._to_data(value))

    def replace(self, key, value):
        """
        Transactional implementation of :func:`Map.replace(key, value) <hazelcast.proxy.map.Map.replace>`

        The object to be replaced will be accessible only in the current transaction context till the transaction is
        committed.

        :param key: (object), the specified key.
        :param value: (object), the value to replace the previous value.
        :return: (object), previous value associated with key, or ``None`` if there was no mapping for key.
        """
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        return self._encode_invoke(transactional_map_replace_codec, key=self._to_data(key),
                                   value=self._to_data(value))

    def replace_if_same(self, key, old_value, new_value):
        """
        Transactional implementation of :func:`Map.replace_if_same(key, old_value, new_value)
        <hazelcast.proxy.map.Map.replace_if_same>`

        The object to be replaced will be accessible only in the current transaction context till the transaction is
        committed.

        :param key: (object), the specified key.
        :param old_value: (object), replace the key value if it is the old value.
        :param new_value: (object), the new value to replace the old value.
        :return: (bool), ``true`` if the value was replaced, ``false`` otherwise.
        """
        check_not_none(key, "key can't be none")
        check_not_none(old_value, "old_value can't be none")
        check_not_none(new_value, "new_value can't be none")
        return self._encode_invoke(transactional_map_replace_if_same_codec, key=self._to_data(key),
                                   old_value=self._to_data(old_value), new_value=self._to_data(new_value))

    def remove(self, key):
        """
        Transactional implementation of :func:`Map.remove(key) <hazelcast.proxy.map.Map.remove>`

        The object to be removed will be removed from only the current transaction context until the transaction is
        committed.

        :param key: (object), key of the mapping to be deleted.
        :return: (object), the previous value associated with key, or ``None`` if there was no mapping for key.
        """
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_map_remove_codec, key=self._to_data(key))

    def remove_if_same(self, key, value):
        """
        Transactional implementation of :func:`Map.remove_if_same(key, value)
        <hazelcast.proxy.map.Map.remove_if_same>`

        The object to be removed will be removed from only the current transaction context until the transaction is
        committed.

        :param key: (object), the specified key.
        :param value: (object), remove the key if it has this value.
        :return: (bool), ``true`` if the value was removed, ``false`` otherwise.
        """

        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        return self._encode_invoke(transactional_map_remove_if_same_codec, key=self._to_data(key),
                                   value=self._to_data(value))

    def delete(self, key):
        """
        Transactional implementation of :func:`Map.delete(key) <hazelcast.proxy.map.Map.delete>`

        The object to be deleted will be removed from only the current transaction context until the transaction is
        committed.

        :param key: (object), key of the mapping to be deleted.
        """
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_map_delete_codec, key=self._to_data(key))

    def key_set(self, predicate=None):
        """
        Transactional implementation of :func:`Map.key_set(predicate) <hazelcast.proxy.map.Map.key_set>`

        :param predicate: (Predicate), predicate to filter the entries (optional).
        :return: (Sequence), a list of the clone of the keys.

        .. seealso::
            :class:`~hazelcast.serialization.predicate.Predicate` for more info about predicates.
        """

        if predicate:
            return self._encode_invoke(transactional_map_key_set_with_predicate_codec,
                                       predicate=self._to_data(predicate))
        return self._encode_invoke(transactional_map_key_set_codec)

    def values(self, predicate=None):
        """
        Transactional implementation of :func:`Map.values(predicate) <hazelcast.proxy.map.Map.values>`

        :param predicate: (Predicate), predicate to filter the entries (optional).
        :return: (Sequence), a list of clone of the values contained in this map.

        .. seealso::
            :class:`~hazelcast.serialization.predicate.Predicate` for more info about predicates.
        """
        if predicate:
            return self._encode_invoke(transactional_map_values_with_predicate_codec,
                                       predicate=self._to_data(predicate))
        return self._encode_invoke(transactional_map_values_codec)
