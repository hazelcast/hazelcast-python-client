from hazelcast.protocol.codec import transactional_queue_offer_codec, transactional_queue_peek_codec, \
    transactional_queue_poll_codec, transactional_queue_size_codec, transactional_queue_take_codec
from hazelcast.proxy.base import TransactionalProxy
from hazelcast.util import check_not_none, to_millis


class TransactionalQueue(TransactionalProxy):
    def offer(self, item, timeout=0):
        check_not_none(item, "item can't be none")
        return self._encode_invoke(transactional_queue_offer_codec, item=self._to_data(item),
                                   timeout=to_millis(timeout))

    def take(self):
        return self._encode_invoke(transactional_queue_take_codec)

    def poll(self, timeout=0):
        return self._encode_invoke(transactional_queue_poll_codec, timeout=to_millis(timeout))

    def peek(self, timeout=0):
        return self._encode_invoke(transactional_queue_peek_codec, timeout=to_millis(timeout))

    def size(self):
        return self._encode_invoke(transactional_queue_size_codec)
