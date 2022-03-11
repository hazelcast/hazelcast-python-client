import typing

from hazelcast.future import Future
from hazelcast.protocol.codec import (
    transactional_queue_offer_codec,
    transactional_queue_peek_codec,
    transactional_queue_poll_codec,
    transactional_queue_size_codec,
    transactional_queue_take_codec,
)
from hazelcast.proxy.base import TransactionalProxy
from hazelcast.types import ItemType
from hazelcast.util import check_not_none, to_millis, thread_id


class TransactionalQueue(TransactionalProxy, typing.Generic[ItemType]):
    """Transactional implementation of :class:`~hazelcast.proxy.queue.Queue`."""

    def offer(self, item: ItemType, timeout: float = 0) -> Future[bool]:
        """Transactional implementation of
        :func:`Queue.offer(item, timeout) <hazelcast.proxy.queue.Queue.offer>`

        Args:
            item: The item to be added.
            timeout (float): Maximum time in seconds to wait for addition.

        Returns:
            Future[bool]: ``True`` if the element was added to this queue,
            ``False`` otherwise.
        """
        check_not_none(item, "item can't be none")

        item_data = self._to_data(item)
        request = transactional_queue_offer_codec.encode_request(
            self.name, self.transaction.id, thread_id(), item_data, to_millis(timeout)
        )
        return self._invoke(request, transactional_queue_offer_codec.decode_response)

    def take(self) -> Future[ItemType]:
        """Transactional implementation of
        :func:`Queue.take() <hazelcast.proxy.queue.Queue.take>`

        Returns:
            Future[any]: The head of this queue.
        """

        def handler(message):
            return self._to_object(transactional_queue_take_codec.decode_response(message))

        request = transactional_queue_take_codec.encode_request(
            self.name, self.transaction.id, thread_id()
        )
        return self._invoke(request, handler)

    def poll(self, timeout: float = 0) -> Future[typing.Optional[ItemType]]:
        """Transactional implementation of
        :func:`Queue.poll(timeout) <hazelcast.proxy.queue.Queue.poll>`

        Args:
            timeout (float): Maximum time in seconds to wait for addition.

        Returns:
            Future[any]: The head of this queue, or ``None`` if this queue is
            empty or specified timeout elapses before an item is added to the
            queue.
        """

        def handler(message):
            return self._to_object(transactional_queue_poll_codec.decode_response(message))

        request = transactional_queue_poll_codec.encode_request(
            self.name, self.transaction.id, thread_id(), to_millis(timeout)
        )
        return self._invoke(request, handler)

    def peek(self, timeout: float = 0) -> Future[typing.Optional[ItemType]]:
        """Transactional implementation of
        :func:`Queue.peek(timeout) <hazelcast.proxy.queue.Queue.peek>`

        Args:
            timeout (float): Maximum time in seconds to wait for addition.

        Returns:
            Future[any]: The head of this queue, or ``None`` if this queue is
            empty or specified timeout elapses before an item is added to the
            queue.
        """

        def handler(message):
            return self._to_object(transactional_queue_peek_codec.decode_response(message))

        request = transactional_queue_peek_codec.encode_request(
            self.name, self.transaction.id, thread_id(), to_millis(timeout)
        )
        return self._invoke(request, handler)

    def size(self) -> Future[int]:
        """Transactional implementation of
        :func:`Queue.size() <hazelcast.proxy.queue.Queue.size>`

        Returns:
            Future[int]: Size of the queue.
        """
        request = transactional_queue_size_codec.encode_request(
            self.name, self.transaction.id, thread_id()
        )
        return self._invoke(request, transactional_queue_size_codec.decode_response)
