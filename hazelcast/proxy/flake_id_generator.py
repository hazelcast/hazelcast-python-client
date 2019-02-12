import itertools
import threading
import collections

from hazelcast.proxy.base import Proxy, MAX_SIZE
from hazelcast.config import FlakeIdGeneratorConfig
from hazelcast.util import current_time_in_millis, TimeUnit, to_millis
from hazelcast.protocol.codec import flake_id_generator_new_id_batch_codec
from hazelcast.future import ImmediateFuture, Future


class FlakeIdGenerator(Proxy):
    """
    A cluster-wide unique ID generator. Generated IDs are int (long in case of the Python 2 on 32 bit architectures)
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

    **Node ID overflow**

    It is possible to generate IDs on any member or client as long as there is at least one
    member with join version smaller than 2^16 in the cluster. The remedy is to restart the cluster:
    nodeId will be assigned from zero again. Uniqueness after the restart will be preserved thanks to
    the timestamp component.

    Requires Hazelcast IMDG 3.10
    """
    _BITS_NODE_ID = 16
    _BITS_SEQUENCE = 6

    def __init__(self, client, service_name, name):
        super(FlakeIdGenerator, self).__init__(client, service_name, name)

        config = client.config.flake_id_generator_configs.get(name, None)
        if config is None:
            config = FlakeIdGeneratorConfig()

        self._auto_batcher = _AutoBatcher(config.prefetch_count, config.prefetch_validity_in_millis,
                                          self._new_id_batch)

    def new_id(self):
        """
        Generates and returns a cluster-wide unique ID.

        This method goes to a random member and gets a batch of IDs, which will then be returned
        locally for limited time. The pre-fetch size and the validity time can be configured, see
        :class:`hazelcast.config.FlakeIdGeneratorConfig`.

        Note: Values returned from this method may not be strictly ordered.

        :raises HazelcastError: if node ID for all members in the cluster is out of valid range.
            See "Node ID overflow" note above.
        :raises UnsupportedOperationError: if the cluster version is below 3.10.

        :return: (int), new cluster-wide unique ID.
        """
        return self._auto_batcher.new_id()

    def init(self, id):
        """
        This method does nothing and will simply tell if the next ID
        will be larger than the given ID.
        You don't need to call this method on cluster restart - uniqueness is preserved thanks to the
        timestamp component of the ID.
        This method exists to make :class:`~hazelcast.proxy.FlakeIdGenerator` drop-in replacement
        for the deprecated :class:`~hazelcast.proxy.IdGenerator`.

        :param id: (int), ID to compare.
        :return: (bool), True if the next ID will be larger than the supplied id, False otherwise.
        """

        # Add 1 hour worth of IDs as a reserve: due to long batch validity some clients might be still getting
        # older IDs. 1 hour is just a safe enough value, not a real guarantee: some clients might have longer
        # validity.
        # The init method should normally be called before any client generated IDs: in this case no reserve is
        # needed, so we don't want to increase the reserve excessively.
        reserve = to_millis(TimeUnit.HOUR) << (FlakeIdGenerator._BITS_NODE_ID + FlakeIdGenerator._BITS_SEQUENCE)
        return self.new_id().continue_with(lambda f: f.result() >= (id + reserve))

    def _new_id_batch(self, batch_size):
        future = self._encode_invoke(flake_id_generator_new_id_batch_codec, self._response_handler,
                                     batch_size=batch_size)

        return future.continue_with(self._response_to_id_batch)

    def _response_handler(self, future, codec, to_object):
        response = future.result()
        if response:
            return codec.decode_response(response, to_object)

    def _response_to_id_batch(self, future):
        response = future.result()
        if response:
            return _IdBatch(response["base"], response["increment"], response["batch_size"])


class _AutoBatcher(object):
    def __init__(self, batch_size, validity_in_millis, id_generator):
        self._batch_size = batch_size
        self._validity_in_millis = validity_in_millis
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
            block = _Block(id_batch, self._validity_in_millis)
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
    def __init__(self, id_batch, validity_in_millis):
        self._id_batch = id_batch
        self._iterator = iter(self._id_batch)
        self._invalid_since = validity_in_millis + current_time_in_millis() if validity_in_millis > 0 else MAX_SIZE

    def next_id(self):
        if self._invalid_since <= current_time_in_millis():
            return None

        return next(self._iterator, None)
