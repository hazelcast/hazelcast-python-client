import typing

from hazelcast.protocol.codec import (
    ringbuffer_add_all_codec,
    ringbuffer_add_codec,
    ringbuffer_capacity_codec,
    ringbuffer_head_sequence_codec,
    ringbuffer_read_many_codec,
    ringbuffer_read_one_codec,
    ringbuffer_remaining_capacity_codec,
    ringbuffer_size_codec,
    ringbuffer_tail_sequence_codec,
)
from hazelcast.internal.asyncio_proxy.base import PartitionSpecificProxy
from hazelcast.proxy.ringbuffer import ReadResult, OVERFLOW_POLICY_OVERWRITE, MAX_BATCH_SIZE
from hazelcast.types import ItemType
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import (
    check_not_negative,
    check_not_none,
    check_not_empty,
    check_true,
    deserialize_list_in_place,
)


class Ringbuffer(PartitionSpecificProxy, typing.Generic[ItemType]):
    """A Ringbuffer is an append-only data-structure where the content is
    stored in a ring like structure.

    A ringbuffer has a capacity so it won't grow beyond that capacity and
    endanger the stability of the system. If that capacity is exceeded, then
    the oldest item in the ringbuffer is overwritten. The ringbuffer has two
    always incrementing sequences:

    - :func:`tail_sequence`: This is the side where the youngest item is found.
      So the tail is the side of the ringbuffer where items are added to.
    - :func:`head_sequence`: This is the side where the oldest items are found.
      So the head is the side where items gets discarded.

    The items in the ringbuffer can be found by a sequence that is in between
    (inclusive) the head and tail sequence.

    If data is read from a ringbuffer with a sequence that is smaller than the
    head sequence, it means that the data is not available anymore and a
    :class:`hazelcast.errors.StaleSequenceError` is thrown.

    A Ringbuffer currently is a replicated, but not partitioned data structure.
    So all data is stored in a single partition, similarly to the
    :class:`hazelcast.internal.asyncio_proxy.queue.Queue` implementation.

    A Ringbuffer can be used in a way similar to the Queue, but one of the key
    differences is that a :func:`hazelcast.internal.asyncio_proxy.queue.Queue.take`
    is destructive, meaning that only 1 consumer is able to take an item.
    A :func:`read_one` is not destructive, so you can have multiple consumers reading the
    same item multiple times.

    Example:
        >>> rb = await client.get_ringbuffer("my_ringbuffer")
        >>> await rb.add("item")
        >>> print("read_one", await rb.read_one(0))
    """

    def __init__(self, service_name, name, context):
        super(Ringbuffer, self).__init__(service_name, name, context)
        self._capacity = None

    async def capacity(self) -> int:
        """Returns the capacity of this Ringbuffer.

        Returns:
            The capacity of Ringbuffer.
        """
        if not self._capacity:

            def handler(message):
                self._capacity = ringbuffer_capacity_codec.decode_response(message)
                return self._capacity

            request = ringbuffer_capacity_codec.encode_request(self.name)
            return await self._invoke(request, handler)

        return self._capacity

    async def size(self) -> int:
        """Returns number of items in the Ringbuffer.

        Returns:
            The size of Ringbuffer.
        """
        request = ringbuffer_size_codec.encode_request(self.name)
        return await self._invoke(request, ringbuffer_size_codec.decode_response)

    async def tail_sequence(self) -> int:
        """Returns the sequence of the tail.

        The tail is the side of the Ringbuffer where the items are added to.
        The initial value of the tail is ``-1``.

        Returns:
            The sequence of the tail.
        """
        request = ringbuffer_tail_sequence_codec.encode_request(self.name)
        return await self._invoke(request, ringbuffer_tail_sequence_codec.decode_response)

    async def head_sequence(self) -> int:
        """Returns the sequence of the head.

        The head is the side of the Ringbuffer where the oldest items in the
        Ringbuffer are found. If the Ringbuffer is empty, the head will be one
        more than the tail. The initial value of the head is ``0`` (``1`` more
        than tail).

        Returns:
            The sequence of the head.
        """
        request = ringbuffer_head_sequence_codec.encode_request(self.name)
        return await self._invoke(request, ringbuffer_head_sequence_codec.decode_response)

    async def remaining_capacity(self) -> int:
        """Returns the remaining capacity of the Ringbuffer.

        Returns:
            The remaining capacity of Ringbuffer.
        """
        request = ringbuffer_remaining_capacity_codec.encode_request(self.name)
        return await self._invoke(request, ringbuffer_remaining_capacity_codec.decode_response)

    async def add(self, item, overflow_policy: int = OVERFLOW_POLICY_OVERWRITE) -> int:
        """Adds the specified item to the tail of the Ringbuffer.

        If there is no space in the Ringbuffer, the action is determined by
        ``overflow_policy``.

        Args:
            item: The specified item to be added.
            overflow_policy: the OverflowPolicy to be used when there is no
                space.

        Returns:
            The sequenceId of the added item, or ``-1`` if the add failed.
        """
        try:
            item_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.add, item, overflow_policy)

        request = ringbuffer_add_codec.encode_request(self.name, overflow_policy, item_data)
        return await self._invoke(request, ringbuffer_add_codec.decode_response)

    async def add_all(
        self,
        items: typing.Sequence[ItemType],
        overflow_policy: int = OVERFLOW_POLICY_OVERWRITE,
    ) -> int:
        """Adds all items in the specified collection to the tail of the
        Ringbuffer.

        This is likely to outperform multiple calls to :func:`add` due
        to better io utilization and a reduced number of executed operations.
        The items are added in the order of the Iterator of the collection.

        If there is no space in the Ringbuffer, the action is determined by
        ``overflow_policy``.

        Args:
            items: The specified collection which contains the items to be
                added.
            overflow_policy: The OverflowPolicy to be used when there is no
                space.

        Returns:
            The sequenceId of the last written item, or ``-1`` of the last
            write is failed.
        """
        check_not_empty(items, "items can't be empty")
        if len(items) > MAX_BATCH_SIZE:
            raise AssertionError("Batch size can't be greater than %d" % MAX_BATCH_SIZE)

        try:
            item_data_list = []
            for item in items:
                check_not_none(item, "item can't be None")
                item_data_list.append(self._to_data(item))
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.add_all, items, overflow_policy)

        request = ringbuffer_add_all_codec.encode_request(
            self.name, item_data_list, overflow_policy
        )
        return await self._invoke(request, ringbuffer_add_all_codec.decode_response)

    async def read_one(self, sequence: int) -> ItemType:
        """Reads one item from the Ringbuffer.

        If the sequence is one beyond the current tail, this call blocks until
        an item  is added. Currently, it isn't possible to control how long
        this call is going to block.

        Args:
            sequence: The sequence of the item to read.

        Returns:
            The read item.
        """
        check_not_negative(sequence, "sequence can't be smaller than 0")

        def handler(message):
            return self._to_object(ringbuffer_read_one_codec.decode_response(message))

        request = ringbuffer_read_one_codec.encode_request(self.name, sequence)
        return await self._invoke(request, handler)

    async def read_many(
        self, start_sequence: int, min_count: int, max_count: int, filter: typing.Any = None
    ) -> ReadResult:
        """Reads a batch of items from the Ringbuffer.

        If the number of available items after the first read item is smaller
        than ``max_count``, these items are returned. So, number of items
        read may be smaller than ``max_count``. If there are
        fewer items available than ``min_count``, then this call blocks.

        Warnings:
            These blocking calls consume server memory and if there are many
            calls, an ``OutOfMemoryError`` may be thrown on server-side.

        Reading a batch of items is likely to perform better because less
        overhead is involved.

        A filter can be provided to select items that need to be read. If
        the filter is ``None``, all items are read. If the filter is not
        ``None``, items where the filter function returns true are
        returned. Using  filters is a good way to prevent getting items that
        are of no value to the receiver. This reduces the amount of IO and the
        number of operations being executed, and can result in a significant
        performance improvement. Note that, filtering logic must be defined
        on the server-side.

        If the ``start_sequence`` is smaller than the smallest sequence still
        available in the Ringbuffer (:func:`head_sequence`), then the smallest
        available sequence will be used as the start sequence and the
        minimum/maximum number of items will be attempted to be read from there
        on.

        If the ``start_sequence`` is bigger than the last available sequence
        in the Ringbuffer (:func:`tail_sequence`), then the last available
        sequence plus one will be used as the start sequence and the call will
        block until further items become available and it can read at least the
        minimum number of items.

        Args:
            start_sequence: The start sequence of the first item to read.
            min_count: The minimum number of items to read.
            max_count: The maximum number of items to read.
            filter: Filter to select returned elements.

        Returns:
            The list of read items.
        """
        check_not_negative(start_sequence, "sequence can't be smaller than 0")
        check_not_negative(min_count, "min count can't be smaller than 0")
        check_true(max_count >= min_count, "max count should be greater or equal to min count")
        check_true(
            max_count < MAX_BATCH_SIZE, "max count can't be greater than %d" % MAX_BATCH_SIZE
        )
        try:
            filter_data = self._to_data(filter)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(
                e, self.read_many, start_sequence, min_count, max_count, filter
            )

        # Since the first call to capacity is cached on the client-side,
        # doing a capacity check each time should not be a problem.
        capacity = await self.capacity()
        check_true(
            max_count <= capacity,
            "max count: %d should be smaller or equal to capacity: %d" % (max_count, capacity),
        )

        request = ringbuffer_read_many_codec.encode_request(
            self.name, start_sequence, min_count, max_count, filter_data
        )

        def handler(message):
            response = ringbuffer_read_many_codec.decode_response(message)
            items = deserialize_list_in_place(response["items"], self._to_object)
            read_count = response["read_count"]
            next_seq = response["next_seq"]
            item_seqs = response["item_seqs"]
            return ReadResult(read_count, next_seq, item_seqs, items)

        return await self._invoke(request, handler)


async def create_ringbuffer_proxy(service_name, name, context):
    return Ringbuffer(service_name, name, context)
