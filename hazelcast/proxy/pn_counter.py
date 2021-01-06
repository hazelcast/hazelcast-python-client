import functools
import logging
import random

from hazelcast.future import Future
from hazelcast.proxy.base import Proxy
from hazelcast.cluster import VectorClock
from hazelcast.protocol.codec import (
    pn_counter_add_codec,
    pn_counter_get_codec,
    pn_counter_get_configured_replica_count_codec,
)
from hazelcast.errors import NoDataMemberInClusterError
from hazelcast.six.moves import range

_logger = logging.getLogger(__name__)


class PNCounter(Proxy):
    """PN (Positive-Negative) CRDT counter.

    The counter supports adding and subtracting values as well as
    retrieving the current counter value.
    Each replica of this counter can perform operations locally without
    coordination with the other replicas, thus increasing availability.
    The counter guarantees that whenever two nodes have received the
    same set of updates, possibly in a different order, their state is
    identical, and any conflicting updates are merged automatically.
    If no new updates are made to the shared state, all nodes that can
    communicate will eventually have the same data.

    When invoking updates from the client, the invocation is remote.
    This may lead to indeterminate state - the update may be applied but the
    response has not been received. In this case, the caller will be notified
    with a TargetDisconnectedError.

    The read and write methods provide monotonic read and RYW (read-your-write)
    guarantees. These guarantees are session guarantees which means that if
    no replica with the previously observed state is reachable, the session
    guarantees are lost and the method invocation will throw a
    ConsistencyLostError. This does not mean
    that an update is lost. All of the updates are part of some replica and
    will be eventually reflected in the state of all other replicas. This
    exception just means that you cannot observe your own writes because
    all replicas that contain your updates are currently unreachable.
    After you have received a ConsistencyLostError, you can either
    wait for a sufficiently up-to-date replica to become reachable in which
    case the session can be continued or you can reset the session by calling
    the reset() method. If you have called the reset() method,
    a new session is started with the next invocation to a CRDT replica.

    Notes:
        The CRDT state is kept entirely on non-lite (data) members. If there
        aren't any and the methods here are invoked on a lite member, they will
        fail with an NoDataMemberInClusterError.
    """

    _EMPTY_ADDRESS_LIST = []

    def __init__(self, service_name, name, context):
        super(PNCounter, self).__init__(service_name, name, context)
        self._observed_clock = VectorClock()
        self._max_replica_count = 0
        self._current_target_replica_address = None

    def get(self):
        """Returns the current value of the counter.

        Returns:
            hazelcast.future.Future[int]: The current value of the counter.

        Raises:
            NoDataMemberInClusterError: if the cluster does not contain any data members.
            ConsistencyLostError: if the session guarantees have been lost.
        """
        return self._invoke_internal(pn_counter_get_codec)

    def get_and_add(self, delta):
        """Adds the given value to the current value and returns the previous value.

        Args:
          delta (int): The value to add.

        Returns:
            hazelcast.future.Future[int]: The previous value.

        Raises:
            NoDataMemberInClusterError: if the cluster does not contain any data members.
            ConsistencyLostError: if the session guarantees have been lost.
        """

        return self._invoke_internal(pn_counter_add_codec, delta=delta, get_before_update=True)

    def add_and_get(self, delta):
        """Adds the given value to the current value and returns the updated value.

        Args:
            delta (int): The value to add.

        Returns:
            hazelcast.future.Future[int]: The updated value.

        Raises:
            NoDataMemberInClusterError: if the cluster does not contain any data members.
            ConsistencyLostError: if the session guarantees have been lost.
        """

        return self._invoke_internal(pn_counter_add_codec, delta=delta, get_before_update=False)

    def get_and_subtract(self, delta):
        """Subtracts the given value from the current value and returns the previous value.

        Args:
            delta (int): The value to subtract.

        Returns:
            hazelcast.future.Future[int]: The previous value.

        Raises:
            NoDataMemberInClusterError: if the cluster does not contain any data members.
            ConsistencyLostError: if the session guarantees have been lost.
        """

        return self._invoke_internal(pn_counter_add_codec, delta=-1 * delta, get_before_update=True)

    def subtract_and_get(self, delta):
        """Subtracts the given value from the current value and returns the updated value.

        Args:
            delta (int): The value to subtract.

        Returns:
            hazelcast.future.Future[int]: The updated value.

        Raises:
            NoDataMemberInClusterError: if the cluster does not contain any data members.
            ConsistencyLostError: if the session guarantees have been lost.
        """

        return self._invoke_internal(
            pn_counter_add_codec, delta=-1 * delta, get_before_update=False
        )

    def get_and_decrement(self):
        """Decrements the counter value by one and returns the previous value.

        Returns:
            hazelcast.future.Future[int]: The previous value.

        Raises:
            NoDataMemberInClusterError: if the cluster does not contain any data members.
            ConsistencyLostError: if the session guarantees have been lost.
        """

        return self._invoke_internal(pn_counter_add_codec, delta=-1, get_before_update=True)

    def decrement_and_get(self):
        """Decrements the counter value by one and returns the updated value.

        Returns:
            hazelcast.future.Future[int]: The updated value.

        Raises:
            NoDataMemberInClusterError: if the cluster does not contain any data members.
            ConsistencyLostError: if the session guarantees have been lost.
        """

        return self._invoke_internal(pn_counter_add_codec, delta=-1, get_before_update=False)

    def get_and_increment(self):
        """Increments the counter value by one and returns the previous value.

        Returns:
            hazelcast.future.Future[int]: The previous value.

        Raises:
            NoDataMemberInClusterError: if the cluster does not contain any data members.
            ConsistencyLostError: if the session guarantees have been lost.
        """

        return self._invoke_internal(pn_counter_add_codec, delta=1, get_before_update=True)

    def increment_and_get(self):
        """Increments the counter value by one and returns the updated value.

        Returns:
            hazelcast.future.Future[int]: The updated value.

        Raises:
            NoDataMemberInClusterError: if the cluster does not contain any data members.
            UnsupportedOperationError: if the cluster version is less than 3.10.
            ConsistencyLostError: if the session guarantees have been lost.
        """

        return self._invoke_internal(pn_counter_add_codec, delta=1, get_before_update=False)

    def reset(self):
        """Resets the observed state by this PN counter.

        This method may be used after a method invocation has thrown a ``ConsistencyLostError``
        to reset the proxy and to be able to start a new session.
        """

        self._observed_clock = VectorClock()

    def _invoke_internal(self, codec, **kwargs):
        delegated_future = Future()
        self._set_result_or_error(
            delegated_future, PNCounter._EMPTY_ADDRESS_LIST, None, codec, **kwargs
        )
        return delegated_future

    def _set_result_or_error(
        self, delegated_future, excluded_addresses, last_error, codec, **kwargs
    ):
        target = self._get_crdt_operation_target(excluded_addresses)
        if not target:
            if last_error:
                delegated_future.set_exception(last_error)
                return
            delegated_future.set_exception(
                NoDataMemberInClusterError(
                    "Cannot invoke operations on a CRDT because "
                    "the cluster does not contain any data members"
                )
            )
            return
        request = codec.encode_request(
            name=self.name,
            replica_timestamps=self._observed_clock.entry_set(),
            target_replica_uuid=target.uuid,
            **kwargs
        )

        future = self._invoke_on_target(request, target.uuid, codec.decode_response)

        checker_func = functools.partial(
            self._check_invocation_result,
            delegated_future=delegated_future,
            excluded_addresses=excluded_addresses,
            target=target,
            codec=codec,
            **kwargs
        )
        future.add_done_callback(checker_func)

    def _check_invocation_result(
        self, future, delegated_future, excluded_addresses, target, codec, **kwargs
    ):
        try:
            result = future.result()
            self._update_observed_replica_timestamp(result["replica_timestamps"])
            delegated_future.set_result(result["value"])
        except Exception as ex:
            _logger.exception(
                "Exception occurred while invoking operation on target %s, "
                "choosing different target",
                target,
            )
            if excluded_addresses == PNCounter._EMPTY_ADDRESS_LIST:
                excluded_addresses = []

            excluded_addresses.append(target)
            self._set_result_or_error(delegated_future, excluded_addresses, ex, codec, **kwargs)

    def _get_crdt_operation_target(self, excluded_addresses):
        if (
            self._current_target_replica_address
            and self._current_target_replica_address not in excluded_addresses
        ):
            return self._current_target_replica_address

        self._current_target_replica_address = self._choose_target_replica(excluded_addresses)
        return self._current_target_replica_address

    def _choose_target_replica(self, excluded_addresses):
        replica_addresses = self._get_replica_addresses(excluded_addresses)

        if len(replica_addresses) == 0:
            return None

        random_replica_index = random.randrange(0, len(replica_addresses))
        return replica_addresses[random_replica_index]

    def _get_replica_addresses(self, excluded_addresses):
        data_members = self._context.cluster_service.get_members(
            lambda member: not member.lite_member
        )
        replica_count = self._get_max_configured_replica_count()

        current_count = min(replica_count, len(data_members))
        replica_addresses = []

        for i in range(current_count):
            member_address = data_members[i]
            if member_address not in excluded_addresses:
                replica_addresses.append(member_address)

        return replica_addresses

    def _get_max_configured_replica_count(self):
        if self._max_replica_count > 0:
            return self._max_replica_count

        request = pn_counter_get_configured_replica_count_codec.encode_request(self.name)
        count = self._invoke(
            request, pn_counter_get_configured_replica_count_codec.decode_response
        ).result()
        self._max_replica_count = count
        return self._max_replica_count

    def _update_observed_replica_timestamp(self, observed_timestamps):
        observed_clock = self._to_vector_clock(observed_timestamps)
        if observed_clock.is_after(self._observed_clock):
            self._observed_clock = observed_clock

    def _to_vector_clock(self, timestamps):
        vector_clock = VectorClock()
        for replica_id, timestamp in timestamps:
            vector_clock.set_replica_timestamp(replica_id, timestamp)

        return vector_clock
