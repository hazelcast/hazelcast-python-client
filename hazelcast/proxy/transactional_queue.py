import typing

from hazelcast.protocol.codec import (
    transactional_queue_offer_codec,
    transactional_queue_peek_codec,
    transactional_queue_poll_codec,
    transactional_queue_size_codec,
    transactional_queue_take_codec,
)
from hazelcast.proxy.base import TransactionalProxy
from hazelcast.types import ItemType
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import check_not_none, to_millis, thread_id


class TransactionalQueue(TransactionalProxy, typing.Generic[ItemType]):
    """Transactional implementation of :class:`~hazelcast.proxy.queue.Queue`."""

    def offer(self, item: ItemType, timeout: float = 0) -> bool:
        """Transactional implementation of
        :func:`Queue.offer(item, timeout) <hazelcast.proxy.queue.Queue.offer>`

        Args:
            item: The item to be added.
            timeout: Maximum time in seconds to wait for addition.

        Returns:
            ``True`` if the element was added to this queue, ``False``
            otherwise.
        """
        check_not_none(item, "item can't be none")
        try:
            item_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.offer(item, timeout)

        request = transactional_queue_offer_codec.encode_request(
            self.name, self.transaction.id, thread_id(), item_data, to_millis(timeout)
        )
        return self._invoke(request, transactional_queue_offer_codec.decode_response)

    def take(self) -> ItemType:
        """Transactional implementation of
        :func:`Queue.take() <hazelcast.proxy.queue.Queue.take>`

        Returns:
            The head of this queue.
        """

        def handler(message):
            return self._to_object(transactional_queue_take_codec.decode_response(message))

        request = transactional_queue_take_codec.encode_request(
            self.name, self.transaction.id, thread_id()
        )
        return self._invoke(request, handler)

    def poll(self, timeout: float = 0) -> typing.Optional[ItemType]:
        """Transactional implementation of
        :func:`Queue.poll(timeout) <hazelcast.proxy.queue.Queue.poll>`

        Args:
            timeout: Maximum time in seconds to wait for addition.

        Returns:
            The head of this queue, or ``None`` if this queue is empty or
            specified timeout elapses before an item is added to the queue.
        """

        def handler(message):
            return self._to_object(transactional_queue_poll_codec.decode_response(message))

        request = transactional_queue_poll_codec.encode_request(
            self.name, self.transaction.id, thread_id(), to_millis(timeout)
        )
        return self._invoke(request, handler)

    def peek(self, timeout: float = 0) -> typing.Optional[ItemType]:
        """Transactional implementation of
        :func:`Queue.peek(timeout) <hazelcast.proxy.queue.Queue.peek>`

        Args:
            timeout: Maximum time in seconds to wait for addition.

        Returns:
            The head of this queue, or ``None`` if this queue is empty or
            specified timeout elapses before an item is added to the queue.
        """

        def handler(message):
            return self._to_object(transactional_queue_peek_codec.decode_response(message))

        request = transactional_queue_peek_codec.encode_request(
            self.name, self.transaction.id, thread_id(), to_millis(timeout)
        )
        return self._invoke(request, handler)

    def size(self) -> int:
        """Transactional implementation of
        :func:`Queue.size() <hazelcast.proxy.queue.Queue.size>`

        Returns:
            Size of the queue.
        """
        request = transactional_queue_size_codec.encode_request(
            self.name, self.transaction.id, thread_id()
        )
        return self._invoke(request, transactional_queue_size_codec.decode_response)
