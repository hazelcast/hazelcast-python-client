from hazelcast.protocol.codec import transactional_set_add_codec, transactional_set_remove_codec, \
    transactional_set_size_codec
from hazelcast.proxy.base import TransactionalProxy
from hazelcast.util import check_not_none


class TransactionalSet(TransactionalProxy):
    def add(self, item):
        check_not_none(item, "item can't be none")
        return self._encode_invoke(transactional_set_add_codec, item=self._to_data(item))

    def remove(self, item):
        check_not_none(item, "item can't be none")
        return self._encode_invoke(transactional_set_remove_codec, item=self._to_data(item))

    def size(self):
        return self._encode_invoke(transactional_set_size_codec)
