from hazelcast.protocol.codec import transactional_multi_map_get_codec, transactional_multi_map_put_codec, \
    transactional_multi_map_remove_codec, transactional_multi_map_remove_entry_codec, \
    transactional_multi_map_size_codec, transactional_multi_map_value_count_codec
from hazelcast.proxy.base import TransactionalProxy
from hazelcast.util import check_not_none


class TransactionalMultiMap(TransactionalProxy):
    def put(self, key, value):
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        return self._encode_invoke(transactional_multi_map_put_codec, key=self._to_data(key),
                                   value=self._to_data(value))

    def get(self, key):
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_multi_map_get_codec, key=self._to_data(key))

    def remove(self, key, value):
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        return self._encode_invoke(transactional_multi_map_remove_entry_codec, key=self._to_data(key),
                                   value=self._to_data(value))

    def remove_all(self, key):
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_multi_map_remove_codec, key=self._to_data(key))

    def value_count(self, key):
        check_not_none(key, "key can't be none")
        return self._encode_invoke(transactional_multi_map_value_count_codec, key=self._to_data(key))

    def size(self):
        return self._encode_invoke(transactional_multi_map_size_codec)
