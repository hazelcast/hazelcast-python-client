import random

from hazelcast.core import Address, DataMemberSelector
from hazelcast.future import ImmediateFuture
from hazelcast.proxy.base import Proxy
from hazelcast.cluster import VectorClock
from hazelcast.protocol.codec import pn_counter_add_codec, pn_counter_get_codec, \
    pn_counter_get_configured_replica_count_codec
from hazelcast.exception import NoDataMemberInClusterError
from hazelcast.six.moves import range


class PNCounter(Proxy):
    _EMPTY_ADDRESS_LIST = []

    def __init__(self, client, service_name, name):
        super(PNCounter, self).__init__(client, service_name, name)
        self._observed_clock = VectorClock()
        self._max_replica_count = 0
        self._current_target_replica_address = None

    def get(self):
        return self._invoke_internal(PNCounter._EMPTY_ADDRESS_LIST,
                                     None,
                                     pn_counter_get_codec)

    def get_and_add(self, delta):
        return self._invoke_internal(PNCounter._EMPTY_ADDRESS_LIST,
                                     None,
                                     pn_counter_add_codec,
                                     delta=delta,
                                     get_before_update=True)

    def add_and_get(self, delta):
        return self._invoke_internal(PNCounter._EMPTY_ADDRESS_LIST,
                                     None,
                                     pn_counter_add_codec,
                                     delta=delta,
                                     get_before_update=False)

    def get_and_subtract(self, delta):
        return self._invoke_internal(PNCounter._EMPTY_ADDRESS_LIST,
                                     None,
                                     pn_counter_add_codec,
                                     delta=-1 * delta,
                                     get_before_update=True)

    def subtract_and_get(self, delta):
        return self._invoke_internal(PNCounter._EMPTY_ADDRESS_LIST,
                                     None,
                                     pn_counter_add_codec,
                                     delta=-1 * delta,
                                     get_before_update=False)

    def get_and_decrement(self):
        return self._invoke_internal(PNCounter._EMPTY_ADDRESS_LIST,
                                     None,
                                     pn_counter_add_codec,
                                     delta=-1,
                                     get_before_update=True)

    def decrement_and_get(self):
        return self._invoke_internal(PNCounter._EMPTY_ADDRESS_LIST,
                                     None,
                                     pn_counter_add_codec,
                                     delta=-1,
                                     get_before_update=False)

    def get_and_increment(self):
        return self._invoke_internal(PNCounter._EMPTY_ADDRESS_LIST,
                                     None,
                                     pn_counter_add_codec,
                                     delta=1,
                                     get_before_update=True)

    def increment_and_get(self):
        return self._invoke_internal(PNCounter._EMPTY_ADDRESS_LIST,
                                     None,
                                     pn_counter_add_codec,
                                     delta=1,
                                     get_before_update=False)

    def reset(self):
        self._observed_clock = VectorClock()

    def _invoke_internal(self, excluded_addresses, last_error, codec, **kwargs):
        target = self._get_crdt_operation_target(excluded_addresses)
        if target is None:
            if last_error:
                raise last_error
            raise NoDataMemberInClusterError("Cannot invoke operations on a CRDT because "
                                             "the cluster does not contain any data members")

        def response_handler(future, codec, to_object):
            response = future.result()
            if response:
                return codec.decode_response(response, to_object)

        try:
            result = self._encode_invoke_on_target(codec,
                                                   target,
                                                   response_handler,
                                                   replica_timestamps=list(self._observed_clock.replica_timestamps.items()),
                                                   target_replica=target,
                                                   **kwargs).result()

            self._update_observed_replica_timestamp(result["replica_timestamps"])
            return ImmediateFuture(result["value"])
        except Exception as ex:
            self.logger.debug("Exception occurred while invoking operation on target {}, "
                              "choosing different target. Cause: {}".format(target, ex))
            if excluded_addresses == PNCounter._EMPTY_ADDRESS_LIST:
                excluded_addresses = []

            excluded_addresses.append(target)
            return self._invoke_internal(excluded_addresses, ex, codec, **kwargs)

    def _get_crdt_operation_target(self, excluded_addresses):
        if self._current_target_replica_address and \
                self._current_target_replica_address not in excluded_addresses:
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
        data_members = self._client.cluster.get_members(DataMemberSelector())
        replica_count = self._get_max_configured_replica_count()

        current_count = min(replica_count, len(data_members))
        replica_addresses = []

        for i in range(current_count):
            member_address = data_members[i].address
            if member_address not in excluded_addresses:
                replica_addresses.append(member_address)

        return replica_addresses

    def _get_max_configured_replica_count(self):
        if self._max_replica_count > 0:
            return self._max_replica_count

        count = self._encode_invoke(pn_counter_get_configured_replica_count_codec).result()
        self._max_replica_count = count
        return self._max_replica_count

    def _update_observed_replica_timestamp(self, observed_timestamps):
        observed_clock = self._to_vector_clock(observed_timestamps)
        if observed_clock.is_after(self._observed_clock):
            self._observed_clock = observed_clock

    def _to_vector_clock(self, timestamps):
        vector_clock = VectorClock()
        for timestamp in timestamps:
            vector_clock.replica_timestamps[timestamp[0]] = timestamp[1]

        return vector_clock
