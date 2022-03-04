import typing

from hazelcast.future import ImmediateFuture, Future
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
from hazelcast.proxy.base import PartitionSpecificProxy
from hazelcast.types import ItemType
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import (
    check_not_negative,
    check_not_none,
    check_not_empty,
    check_true,
    ImmutableLazyDataList,
)

OVERFLOW_POLICY_OVERWRITE = 0
"""
Configuration property for DEFAULT overflow policy. When an item is tried to be added on full Ringbuffer, oldest item in
the Ringbuffer is overwritten and item is added.
"""

OVERFLOW_POLICY_FAIL = 1
"""
Configuration property for overflow policy. When an item is tried to be added on full Ringbuffer, the call fails and
item is not added.

The reason that FAIL exist is to give the opportunity to obey the ttl. If blocking behavior is required, this can be
implemented using retrying in combination with an exponential backoff.

    >>> sleepMS = 100;
    >>> while true:
    >>>     result = ringbuffer.add(item, -1)
    >>>     if result != -1:
    >>>         break
    >>>     sleep(sleepMS / 1000)
    >>>     sleepMS *= 2
"""

MAX_BATCH_SIZE = 1000
"""
The maximum number of items to be added to RingBuffer or read from RingBuffer at a time.
"""


class ReadResult(ImmutableLazyDataList):
    """Defines the result of a :func:`Ringbuffer.read_many` operation."""

    SEQUENCE_UNAVAILABLE = -1
    """Value returned from methods returning a sequence number when the
    information is not available (e.g. because of rolling upgrade and some
    members not returning the sequence).
    """

    def __init__(self, read_count, next_seq, items, item_seqs, to_object):
        super(ReadResult, self).__init__(items, to_object)
        self._read_count = read_count
        self._next_seq = next_seq
        self._item_seqs = item_seqs

    @property
    def read_count(self) -> int:
        """The number of items that have been read before filtering.

        If no filter is set, then the :attr:`read_count` will be equal to
        :attr:`size`.

        But if a filter is applied, it could be that items are read, but are
        filtered out. So, if you are trying to make another read based on
        this, then you should increment the sequence by :attr:`read_count` and
        not by :attr:`size`.

        Otherwise, you will be re-reading the same filtered messages.
        """
        return self._read_count

    @property
    def size(self) -> int:
        """The result set size.

        See Also:
            :attr:`read_count`
        """
        return len(self._list_data)

    @property
    def next_sequence_to_read_from(self) -> int:
        """The sequence of the item following the last read item.

        This sequence can then be used to read items following the ones
        returned by this result set.

        Usually this sequence is equal to the sequence used to retrieve this
        result set incremented by the :attr:`read_count`. In cases when the
        reader tolerates lost items, this is not the case.

        For instance, if the reader requests an item with a stale sequence (one
        which has already been overwritten), the read will jump to the oldest
        sequence and read from there.

        Similarly, if the reader requests an item in the future (e.g. because
        the partition was lost and the reader was unaware of this), the read
        method will jump back to the newest available sequence.

        Because of these jumps and only in the case when the reader is loss
        tolerant, the next sequence must be retrieved using this method.
        A return value of :const:`SEQUENCE_UNAVAILABLE` means that the
        information is not available.
        """
        return self._next_seq

    def get_sequence(self, index: int) -> int:
        """Return the sequence number for the item at the given index.

        Args:
            index: The index.

        Returns:
            The sequence number for the ringbuffer item.
        """
        return self._item_seqs[index]


class Ringbuffer(PartitionSpecificProxy["BlockingRingbuffer"], typing.Generic[ItemType]):
    """A Ringbuffer is an append-only data-structure where the content is
    stored in a ring like structure.

    A ringbuffer has a capacity so it won't grow beyond that capacity and
    endanger the stability of the system. If that capacity is exceeded, than
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
    :class:`hazelcast.proxy.queue.Queue` implementation.

    A Ringbuffer can be used in a way similar to the Queue, but one of the key
    differences is that a :func:`hazelcast.proxy.queue.Queue.take` is destructive,
    meaning that only 1 thread is able to take an item. A :func:`read_one` is not
    destructive, so you can have multiple threads reading the same item multiple
    times.
    """

    def __init__(self, service_name, name, context):
        super(Ringbuffer, self).__init__(service_name, name, context)
        self._capacity = None

    def capacity(self) -> Future[int]:
        """Returns the capacity of this Ringbuffer.

        Returns:
            The capacity of Ringbuffer.
        """
        if not self._capacity:

            def handler(message):
                self._capacity = ringbuffer_capacity_codec.decode_response(message)
                return self._capacity

            request = ringbuffer_capacity_codec.encode_request(self.name)
            return self._invoke(request, handler)

        return ImmediateFuture(self._capacity)

    def size(self) -> Future[int]:
        """Returns number of items in the Ringbuffer.

        Returns:
            The size of Ringbuffer.
        """
        request = ringbuffer_size_codec.encode_request(self.name)
        return self._invoke(request, ringbuffer_size_codec.decode_response)

    def tail_sequence(self) -> Future[int]:
        """Returns the sequence of the tail.

        The tail is the side of the Ringbuffer where the items are added to.
        The initial value of the tail is ``-1``.

        Returns:
            The sequence of the tail.
        """
        request = ringbuffer_tail_sequence_codec.encode_request(self.name)
        return self._invoke(request, ringbuffer_tail_sequence_codec.decode_response)

    def head_sequence(self) -> Future[int]:
        """Returns the sequence of the head.

        The head is the side of the Ringbuffer where the oldest items in the
        Ringbuffer are found. If the Ringbuffer is empty, the head will be one
        more than the tail. The initial value of the head is ``0`` (``1`` more
        than tail).

        Returns:
            The sequence of the head.
        """
        request = ringbuffer_head_sequence_codec.encode_request(self.name)
        return self._invoke(request, ringbuffer_head_sequence_codec.decode_response)

    def remaining_capacity(self) -> Future[int]:
        """Returns the remaining capacity of the Ringbuffer.

        Returns:
            The remaining capacity of Ringbuffer.
        """
        request = ringbuffer_remaining_capacity_codec.encode_request(self.name)
        return self._invoke(request, ringbuffer_remaining_capacity_codec.decode_response)

    def add(self, item, overflow_policy: int = OVERFLOW_POLICY_OVERWRITE) -> Future[int]:
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
            return self._send_schema_and_retry(e, self.add, item, overflow_policy)

        request = ringbuffer_add_codec.encode_request(self.name, overflow_policy, item_data)
        return self._invoke(request, ringbuffer_add_codec.decode_response)

    def add_all(
        self,
        items: typing.Sequence[ItemType],
        overflow_policy: int = OVERFLOW_POLICY_OVERWRITE,
    ) -> Future[int]:
        """Adds all of the item in the specified collection to the tail of the
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
            return self._send_schema_and_retry(e, self.add_all, items, overflow_policy)

        request = ringbuffer_add_all_codec.encode_request(
            self.name, item_data_list, overflow_policy
        )
        return self._invoke(request, ringbuffer_add_all_codec.decode_response)

    def read_one(self, sequence: int) -> Future[ItemType]:
        """Reads one item from the Ringbuffer.

        If the sequence is one beyond the current tail, this call blocks until
        an item  is added. Currently it isn't possible to control how long
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
        return self._invoke(request, handler)

    def read_many(
        self, start_sequence: int, min_count: int, max_count: int, filter: typing.Any = None
    ) -> Future[ReadResult]:
        """Reads a batch of items from the Ringbuffer.

        If the number of available items after the first read item is smaller
        than the ``max_count``, these items are returned. So it could be the
        number of items read is smaller than the ``max_count``. If there are
        less items available than ``min_count``, then this call blocks.

        Warnings:
            These blocking calls consume server memory and if there are many
            calls, it can be possible to see leaking memory or
            ``OutOfMemoryError`` s on the server.

        Reading a batch of items is likely to perform better because less
        overhead is involved.

        A filter can be provided to only select items that need to be read. If
        the filter is ``None``, all items are read. If the filter is not
        ``None``, only items where the filter function returns true are
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
            return self._send_schema_and_retry(
                e, self.read_many, start_sequence, min_count, max_count, filter
            )

        request = ringbuffer_read_many_codec.encode_request(
            self.name, start_sequence, min_count, max_count, filter_data
        )

        def handler(message):
            response = ringbuffer_read_many_codec.decode_response(message)
            read_count = response["read_count"]
            next_seq = response["next_seq"]
            items = response["items"]
            item_seqs = response["item_seqs"]

            return ReadResult(read_count, next_seq, items, item_seqs, self._to_object)

        def continuation(future):
            # Since the first call to capacity
            # is cached on the client-side, doing
            # a capacity check each time should not
            # be a problem
            capacity = future.result()

            check_true(
                max_count <= capacity,
                "max count: %d should be smaller or equal to capacity: %d" % (max_count, capacity),
            )

            return self._invoke(request, handler)

        return self.capacity().continue_with(continuation)

    def blocking(self) -> "BlockingRingbuffer[ItemType]":
        return BlockingRingbuffer(self)


class BlockingRingbuffer(Ringbuffer[ItemType]):
    __slots__ = ("_wrapped", "name", "service_name")

    def __init__(self, wrapped: Ringbuffer[ItemType]):
        self.name = wrapped.name
        self.service_name = wrapped.service_name
        self._wrapped = wrapped

    def capacity(  # type: ignore[override]
        self,
    ) -> int:
        return self._wrapped.capacity().result()

    def size(  # type: ignore[override]
        self,
    ) -> int:
        return self._wrapped.size().result()

    def tail_sequence(  # type: ignore[override]
        self,
    ) -> int:
        return self._wrapped.tail_sequence().result()

    def head_sequence(  # type: ignore[override]
        self,
    ) -> int:
        return self._wrapped.head_sequence().result()

    def remaining_capacity(  # type: ignore[override]
        self,
    ) -> int:
        return self._wrapped.remaining_capacity().result()

    def add(  # type: ignore[override]
        self,
        item,
        overflow_policy: int = OVERFLOW_POLICY_OVERWRITE,
    ) -> int:
        return self._wrapped.add(item, overflow_policy).result()

    def add_all(  # type: ignore[override]
        self,
        items: typing.Sequence[ItemType],
        overflow_policy: int = OVERFLOW_POLICY_OVERWRITE,
    ) -> int:
        return self._wrapped.add_all(items, overflow_policy).result()

    def read_one(  # type: ignore[override]
        self,
        sequence: int,
    ) -> ItemType:
        return self._wrapped.read_one(sequence).result()

    def read_many(  # type: ignore[override]
        self,
        start_sequence: int,
        min_count: int,
        max_count: int,
        filter: typing.Any = None,
    ) -> ReadResult:
        return self._wrapped.read_many(start_sequence, min_count, max_count, filter).result()

    def destroy(self) -> bool:
        return self._wrapped.destroy()

    def blocking(self) -> "BlockingRingbuffer[ItemType]":
        return self

    def __repr__(self) -> str:
        return self._wrapped.__repr__()
