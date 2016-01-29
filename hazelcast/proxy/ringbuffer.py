from hazelcast.future import ImmediateFuture
from hazelcast.protocol.codec import ringbuffer_add_all_codec, ringbuffer_add_codec, ringbuffer_capacity_codec, \
    ringbuffer_head_sequence_codec, ringbuffer_read_many_codec, ringbuffer_read_one_codec, \
    ringbuffer_remaining_capacity_codec, ringbuffer_size_codec, ringbuffer_tail_sequence_codec
from hazelcast.proxy.base import PartitionSpecificProxy
from hazelcast.util import check_not_negative, check_not_none, check_not_empty, check_true

OVERFLOW_POLICY_OVERWRITE = 0
OVERFLOW_POLICY_FAIL = 1
MAX_BATCH_SIZE = 1000


class Ringbuffer(PartitionSpecificProxy):
    _capacity = None

    def capacity(self):
        if not self._capacity:
            def cache_capacity(f):
                self._capacity = f.result()
                return f.result()

            return self._encode_invoke(ringbuffer_capacity_codec).continue_with(cache_capacity)
        return ImmediateFuture(self._capacity)

    def size(self):
        return self._encode_invoke(ringbuffer_size_codec)

    def tail_sequence(self):
        return self._encode_invoke(ringbuffer_tail_sequence_codec)

    def head_sequence(self):
        return self._encode_invoke(ringbuffer_head_sequence_codec)

    def remaining_capacity(self):
        return self._encode_invoke(ringbuffer_remaining_capacity_codec)

    def add(self, item, overflow_policy=OVERFLOW_POLICY_OVERWRITE):
        return self._encode_invoke(ringbuffer_add_codec, value=self._to_data(item), overflow_policy=overflow_policy)

    def add_all(self, items, overflow_policy=OVERFLOW_POLICY_OVERWRITE):
        check_not_empty(items, "items can't be empty")
        if len(items) > MAX_BATCH_SIZE:
            raise AssertionError("Batch size can't be greater than %d" % MAX_BATCH_SIZE)
        for item in items:
            check_not_none(item, "item can't be None")

        item_list = [self._to_data(x) for x in items]
        return self._encode_invoke(ringbuffer_add_all_codec, value_list=item_list, overflow_policy=overflow_policy)

    def read_one(self, sequence):
        check_not_negative(sequence, "sequence can't be smaller than 0")
        return self._encode_invoke(ringbuffer_read_one_codec, sequence=sequence)

    def read_many(self, start_sequence, min_count, max_count):
        check_not_negative(start_sequence, "sequence can't be smaller than 0")
        check_true(max_count >= min_count, "max count should be greater or equal to min count")
        check_true(min_count <= self.capacity().result(), "min count should be smaller or equal to capacity")
        check_true(max_count < MAX_BATCH_SIZE, "max count can't be greater than %d" % MAX_BATCH_SIZE)

        return self._encode_invoke(ringbuffer_read_many_codec, response_handler=self._read_many_response_handler,
                                   start_sequence=start_sequence, min_count=min_count,
                                   max_count=max_count, filter=None)

    @staticmethod
    def _read_many_response_handler(future, codec, to_object):
        return codec.decode_response(future.result(), to_object)['items']
