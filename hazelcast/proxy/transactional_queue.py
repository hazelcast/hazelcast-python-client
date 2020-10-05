from hazelcast.protocol.codec import transactional_queue_offer_codec, transactional_queue_peek_codec, \
    transactional_queue_poll_codec, transactional_queue_size_codec, transactional_queue_take_codec
from hazelcast.proxy.base import TransactionalProxy
from hazelcast.util import check_not_none, to_millis, thread_id


class TransactionalQueue(TransactionalProxy):
    """
    Transactional implementation of :class:`~hazelcast.proxy.queue.Queue`.
    """
    def offer(self, item, timeout=0):
        """
        Transactional implementation of :func:`Queue.offer(item, timeout) <hazelcast.proxy.queue.Queue.offer>`

        :param item: (object), the item to be added.
        :param timeout: (long), maximum time in seconds to wait for addition (optional).
        :return: (bool), ``true`` if the element was added to this queue, ``false`` otherwise.
        """
        check_not_none(item, "item can't be none")

        item_data = self._to_data(item)
        request = transactional_queue_offer_codec.encode_request(self.name, self.transaction.id, thread_id(),
                                                                 item_data, to_millis(timeout))
        return self._invoke(request, transactional_queue_offer_codec.decode_response)

    def take(self):
        """
        Transactional implementation of :func:`Queue.take() <hazelcast.proxy.queue.Queue.take>`

        :return: (object), the head of this queue.
        """
        def handler(message):
            return self._to_object(transactional_queue_take_codec.decode_response(message))

        request = transactional_queue_take_codec.encode_request(self.name, self.transaction.id, thread_id())
        return self._invoke(request, handler)

    def poll(self, timeout=0):
        """
        Transactional implementation of :func:`Queue.poll(timeout) <hazelcast.proxy.queue.Queue.poll>`

        :param timeout: (long), maximum time in seconds to wait for addition (optional).
        :return: (object), the head of this queue, or ``None`` if this queue is empty or specified timeout elapses
            before an item is added to the queue.
        """
        def handler(message):
            return self._to_object(transactional_queue_poll_codec.decode_response(message))

        request = transactional_queue_poll_codec.encode_request(self.name, self.transaction.id, thread_id(),
                                                                to_millis(timeout))
        return self._invoke(request, handler)

    def peek(self, timeout=0):
        """
        Transactional implementation of :func:`Queue.peek(timeout) <hazelcast.proxy.queue.Queue.peek>`

        :param timeout: (long), maximum time in seconds to wait for addition (optional).
        :return: (object), the head of this queue, or ``None`` if this queue is empty or specified timeout elapses
            before an item is added to the queue.
        """
        def handler(message):
            return self._to_object(transactional_queue_peek_codec.decode_response(message))

        request = transactional_queue_peek_codec.encode_request(self.name, self.transaction.id, thread_id(),
                                                                to_millis(timeout))
        return self._invoke(request, handler)

    def size(self):
        """
        Transactional implementation of :func:`Queue.size() <hazelcast.proxy.queue.Queue.size>`

        :return: (int), size of the queue.
        """
        request = transactional_queue_size_codec.encode_request(self.name, self.transaction.id, thread_id())
        return self._invoke(request, transactional_queue_size_codec.decode_response)
