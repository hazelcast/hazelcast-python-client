from hazelcast.protocol.codec import transactional_map_contains_key_codec, transactional_map_delete_codec, \
    transactional_map_get_codec, transactional_map_get_for_update_codec, transactional_map_is_empty_codec, \
    transactional_map_key_set_codec, transactional_map_key_set_with_predicate_codec, transactional_map_put_codec, \
    transactional_map_put_if_absent_codec, transactional_map_remove_codec, transactional_map_remove_if_same_codec, \
    transactional_map_replace_codec, transactional_map_replace_if_same_codec, transactional_map_set_codec, \
    transactional_map_size_codec, transactional_map_values_codec, transactional_map_values_with_predicate_codec
from hazelcast.proxy.base import TransactionalProxy
from hazelcast.util import check_not_none, to_millis


class TransactionalMap(TransactionalProxy):
    def contains_key(self, key):
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_map_contains_key_codec, key=self._to_data(key))

    def get(self, key):
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_map_get_codec, key=self._to_data(key))

    def get_for_update(self, key):
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_map_get_for_update_codec, key=self._to_data(key))

    def size(self):
        return self._encode_invoke(transactional_map_size_codec)

    def is_empty(self):
        return self._encode_invoke(transactional_map_is_empty_codec)

    def put(self, key, value, ttl=-1):
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        return self._encode_invoke(transactional_map_put_codec, key=self._to_data(key),
                                   value=self._to_data(value), ttl=to_millis(ttl))

    def put_if_absent(self, key, value):
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        return self._encode_invoke(transactional_map_put_if_absent_codec, key=self._to_data(key),
                                   value=self._to_data(value))

    def set(self, key, value):
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        return self._encode_invoke(transactional_map_set_codec, key=self._to_data(key),
                                   value=self._to_data(value))

    def replace(self, key, value):
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        return self._encode_invoke(transactional_map_replace_codec, key=self._to_data(key),
                                   value=self._to_data(value))

    def replace_if_same(self, key, old_value, new_value):
        check_not_none(key, "key can't be none")
        check_not_none(old_value, "old_value can't be none")
        check_not_none(new_value, "new_value can't be none")
        return self._encode_invoke(transactional_map_replace_if_same_codec, key=self._to_data(key),
                                   old_value=self._to_data(old_value), new_value=self._to_data(new_value))

    def remove(self, key):
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_map_remove_codec, key=self._to_data(key))

    def remove_if_same(self, key, value):
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        return self._encode_invoke(transactional_map_remove_if_same_codec, key=self._to_data(key),
                                   value=self._to_data(value))

    def delete(self, key):
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_map_delete_codec, key=self._to_data(key))

    def key_set(self, predicate=None):
        if predicate:
            return self._encode_invoke(transactional_map_key_set_with_predicate_codec,
                                       predicate=self._to_data(predicate))
        return self._encode_invoke(transactional_map_key_set_codec)

    def values(self, predicate=None):
        if predicate:
            return self._encode_invoke(transactional_map_values_with_predicate_codec,
                                       predicate=self._to_data(predicate))
        return self._encode_invoke(transactional_map_values_codec)
