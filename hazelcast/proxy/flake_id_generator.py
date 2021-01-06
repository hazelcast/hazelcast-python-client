import itertools
import threading
import collections

from hazelcast.proxy.base import Proxy, MAX_SIZE
from hazelcast.config import _FlakeIdGeneratorConfig
from hazelcast.util import current_time
from hazelcast.protocol.codec import flake_id_generator_new_id_batch_codec
from hazelcast.future import ImmediateFuture, Future


class FlakeIdGenerator(Proxy):
    """A cluster-wide unique ID generator. Generated IDs are int (long in case of the Python 2 on 32 bit architectures)
    values and are k-ordered (roughly ordered). IDs are in the range from 0 to 2^63 - 1.

    The IDs contain timestamp component and a node ID component, which is assigned when the member
    joins the cluster. This allows the IDs to be ordered and unique without any coordination between
    members, which makes the generator safe even in split-brain scenario.

    Timestamp component is in milliseconds since 1.1.2018, 0:00 UTC and has 41 bits. This caps
    the useful lifespan of the generator to little less than 70 years (until ~2088). The sequence component
    is 6 bits. If more than 64 IDs are requested in single millisecond, IDs will gracefully overflow to the next
    millisecond and uniqueness is guaranteed in this case. The implementation does not allow overflowing
    by more than 15 seconds, if IDs are requested at higher rate, the call will block. Note, however, that
    clients are able to generate even faster because each call goes to a different (random) member and
    the 64 IDs/ms limit is for single member.

    Node ID overflow:
        It is possible to generate IDs on any member or client as long as there is at least one
        member with join version smaller than 2^16 in the cluster. The remedy is to restart the cluster:
        nodeId will be assigned from zero again. Uniqueness after the restart will be preserved thanks to
        the timestamp component.
    """

    _BITS_NODE_ID = 16
    _BITS_SEQUENCE = 6

    def __init__(self, service_name, name, context):
        super(FlakeIdGenerator, self).__init__(service_name, name, context)

        config = context.config.flake_id_generators.get(name, None)
        if config is None:
            config = _FlakeIdGeneratorConfig()

        self._auto_batcher = _AutoBatcher(
            config.prefetch_count, config.prefetch_validity, self._new_id_batch
        )

    def new_id(self):
        """Generates and returns a cluster-wide unique ID.

        This method goes to a random member and gets a batch of IDs, which will then be returned
        locally for limited time. The pre-fetch size and the validity time can be configured.

        Note:
            Values returned from this method may not be strictly ordered.

        Returns:
          hazelcast.future.Future[int], new cluster-wide unique ID.

        Raises:
            HazelcastError: if node ID for all members in the cluster is out of valid range.
                See ``Node ID overflow`` note above.
        """
        return self._auto_batcher.new_id()

    def _new_id_batch(self, batch_size):
        def handler(message):
            response = flake_id_generator_new_id_batch_codec.decode_response(message)
            return _IdBatch(response["base"], response["increment"], response["batch_size"])

        request = flake_id_generator_new_id_batch_codec.encode_request(self.name, batch_size)
        return self._invoke(request, handler)


class _AutoBatcher(object):
    def __init__(self, batch_size, validity, id_generator):
        self._batch_size = batch_size
        self._validity = validity
        self._batch_id_supplier = id_generator
        self._block = _Block(_IdBatch(0, 0, 0), 0)
        self._lock = threading.RLock()
        self._id_queue = collections.deque()
        self._request_in_air = False

    def new_id(self):
        while True:
            block = self._block
            next_id = block.next_id()
            if next_id is not None:
                return ImmediateFuture(next_id)

            with self._lock:
                # new block was assigned in the meantime
                if block is not self._block:
                    continue

                future = Future()
                self._id_queue.append(future)
                if not self._request_in_air:
                    self._request_in_air = True
                    self._request_new_batch()
                return future

    def _request_new_batch(self):
        future = self._batch_id_supplier(self._batch_size)
        future.add_done_callback(self._assign_new_block)

    def _assign_new_block(self, future):
        try:
            new_batch_required = False
            id_batch = future.result()
            block = _Block(id_batch, self._validity)
            with self._lock:
                while True:
                    try:
                        f = self._id_queue.popleft()
                        next_id = block.next_id()
                        if next_id is not None:
                            f.set_result(next_id)
                        else:
                            self._id_queue.appendleft(f)
                            new_batch_required = True
                            break
                    except IndexError:
                        break
                if new_batch_required:
                    self._request_in_air = True
                    self._request_new_batch()
                else:
                    self._request_in_air = False
                    self._block = block
        except Exception as ex:
            with self._lock:
                while True:
                    try:
                        f = self._id_queue.popleft()
                        f.set_exception(ex)
                    except IndexError:
                        break
                self._request_in_air = False


class _IdBatch(object):
    def __init__(self, base, increment, batch_size):
        self._base = base
        self._increment = increment
        self._batch_size = batch_size

    def __iter__(self):
        self._remaining = itertools.count(self._batch_size, -1)
        self._next_id = itertools.count(self._base, self._increment)
        return self

    def __next__(self):
        if next(self._remaining) <= 0:
            raise StopIteration

        return next(self._next_id)

    # For Python 2 compatibility
    next = __next__


class _Block(object):
    def __init__(self, id_batch, validity):
        self._id_batch = id_batch
        self._iterator = iter(self._id_batch)
        self._invalid_since = validity + current_time() if validity > 0 else MAX_SIZE

    def next_id(self):
        if self._invalid_since <= current_time():
            return None

        return next(self._iterator, None)
