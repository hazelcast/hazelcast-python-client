import logging
import random
import threading
import time
import uuid

from hazelcast.exception import HazelcastError, AuthenticationError, TargetDisconnectedError
from hazelcast.lifecycle import LIFECYCLE_STATE_CLIENT_CONNECTED, LIFECYCLE_STATE_CLIENT_DISCONNECTED
from hazelcast.protocol.codec import client_authentication_codec
from hazelcast.util import get_possible_addresses, get_provider_addresses, calculate_version
from hazelcast.version import CLIENT_TYPE, CLIENT_VERSION, SERIALIZATION_VERSION
from hazelcast.protocol.codec import client_add_cluster_view_listener_codec
from hazelcast.invocation import Invocation
from hazelcast.core import MemberInfo
from hazelcast.future import Future
import logging
import threading

# Membership Event Types
MEMBER_ADDED = 1
MEMBER_REMOVED = 2


class MemberListSnapshot(object):
    def __init__(self, version, members):
        self.version = version
        self.members = members

    def __len__(self):
        return len(self.members)

    def __str__(self):
        return "MemberListSnapshot"

    def __repr__(self):
        return "MemberListSnapshot"


EMPTY_SNAPSHOT = MemberListSnapshot(-1, {})


class ClusterService(object):
    """
    Hazelcast cluster service. It provides access to the members in the cluster and the client can register for changes
    in the cluster members.

    All the methods on the Cluster are thread-safe.
    """
    logger = logging.getLogger("HazelcastClient.ClusterService")

    def __init__(self, config, client):
        self.semaphore_ = threading.Semaphore()
        self._config = config
        self._client = client
        self._logger_extras = {"client_name": client.name, "cluster_name": config.cluster_name}
        self._members = {}
        self.owner_connection_address = None
        # self.owner_uuid = None
        self.uuid = uuid.uuid4()
        self.listeners = {}
        self.member_list_snapshot = EMPTY_SNAPSHOT

        for listener in config.membership_listeners:
            self.add_listener(*listener)

        # self._address_providers = address_providers
        self._initial_list_fetched = threading.Event()
        # self._client.connection_manager.add_listener(on_connection_closed=self._connection_closed)
        # self._client.heartbeat.add_listener(on_heartbeat_stopped=self._heartbeat_stopped)

    def start(self, configured_listeners):
        for listener in configured_listeners:
            self.add_membership_listener(listener)

    def shutdown(self):
        pass

    def size(self):
        """
        Returns the size of the cluster.

        :return: (int), size of the cluster.
        """
        return len(self._members)

    def add_listener(self, member_added=None, member_removed=None, fire_for_existing=False):
        """
        Adds a membership listener to listen for membership updates, it will be notified when a member is added to
        cluster or removed from cluster. There is no check for duplicate registrations, so if you register the listener
        twice, it will get events twice.


        :param member_added: (Function), function to be called when a member is added to the cluster (optional).
        :param member_removed: (Function), function to be called when a member is removed to the cluster (optional).
        :param fire_for_existing: (bool), (optional).
        :return: (str), registration id of the listener which will be used for removing this listener.
        """
        registration_id = str(uuid.uuid4())
        self.listeners[registration_id] = (member_added, member_removed)

        if fire_for_existing:
            for member in self.get_member_list():
                member_added(member)

        return registration_id

    def remove_listener(self, registration_id):
        """
        Removes the specified membership listener.

        :param registration_id: (str), registration id of the listener to be deleted.
        :return: (bool), if the registration is removed, ``false`` otherwise.
        """
        try:
            self.listeners.pop(registration_id)
            return True
        except KeyError:
            return False

    @property
    def members(self):
        """
        Returns the members in the cluster.
        :return: (list), List of members.
        """
        return self.get_member_list()

    def add_membership_listener(self, listener):
        assert listener

        if isinstance(listener, AbstractLoadBalancer):
            members = self.get_member_list()

            if len(members) != 0:
                event = InitialMembershipEvent(members)
                listener.init(event)

    def _authenticate_manager(self, connection):
        request = client_authentication_codec.encode_request(
            cluster_name=self._config.cluster_name,
            username=None,
            password=None,
            uuid=self.uuid,
            client_type=CLIENT_TYPE,
            serialization_version=SERIALIZATION_VERSION,
            client_hazelcast_version=CLIENT_VERSION,
            client_name=self._client.name,
            labels=[])

        def callback(f):
            parameters = client_authentication_codec.decode_response(f.result())
            if parameters["status"] != 0:  # TODO: handle other statuses
                raise AuthenticationError("Authentication failed.")
            connection.remote_address = parameters["address"]
            connection.is_owner = True
            # self.owner_uuid = parameters["owner_uuid"]
            self.uuid = parameters["memberUuid"]
            self._client.partition_service.partition_count = parameters["partitionCount"]
            connection.server_version_str = parameters.get("serverHazelcastVersion", "")
            connection.server_version = calculate_version(connection.server_version_str)
            return connection

        return self._client.invoker.invoke_on_connection(request, connection).continue_with(callback)

    def _log_member_list(self):
        self.logger.info("New member list:\n\nMembers [%d] {\n%s\n}\n", self.size(),
                         "\n".join(["\t" + str(x) for x in self.get_member_list()]), extra=self._logger_extras)

    def _heartbeat_stopped(self, connection):
        if connection.remote_address == self.owner_connection_address:
            self._client.connection_manager.close_connection(connection.remote_address, TargetDisconnectedError(
                "%s stopped heart beating." % connection))

    def get_member_by_uuid(self, member_uuid):
        """
        Returns the member with specified member uuid if it is in the
        cluster, None otherwise.

        :param member_uuid: (UUID), uuid of the desired member.
        :return: (:class:`~hazelcast.core.Member`), the corresponding member.
        """
        return self._members.get(member_uuid, None)

    def get_member_by_address(self, address):
        """
        Returns the member with the specified address if it is in the
        cluster, None otherwise.

        :param address: (:class:`~hazelcast.core.Address`), address of the desired member.
        :return: (:class:`~hazelcast.core.Member`), the corresponding member.
        """
        for member in self.get_member_list():
            if address == member.address:
                return member
        return None

    def get_members(self, selector):
        """
        Returns the members that satisfy the given selector.

        :param selector: (:class:`~hazelcast.core.MemberSelector`), Selector to be applied to the members.
        :return: (List), List of members.
        """
        members = []
        for member in self.get_member_list():
            if selector.select(member):
                members.append(member)

        return members

    def get_member_list(self):
        """
        Returns all the members as a list.

        :return: (List), List of members.
        """
        return list(self._members.values())

    def wait_initial_membership_fetched(self):
        future = Future()
        future.set_result(self.semaphore_.acquire())
        return future

    def handle_members_view_event(self, version=None, member_infos=None):
        assert version and member_infos
        snapshot = self.create_snapshot(version, member_infos)
        self.logger.debug("Handling new snapshot with membership version: " + str(version),
                          extra=self._logger_extras)

        if self.member_list_snapshot is EMPTY_SNAPSHOT:
            self.apply_initial_state(version, member_infos)

            self.semaphore_.release()
            # self._log_member_list()
            return

        events = []
        if version >= self.member_list_snapshot.version:
            prev_members = self.member_list_snapshot.members
            snapshot = self.create_snapshot(version, member_infos)
            self.member_list_snapshot = snapshot
            current_members = snapshot.members
            events = self.detect_membership_events(prev_members, current_members)

        self.fire_events(events)

    def detect_membership_events(self, prev_member, current_members):
        dead_members = set(prev_member.keys())
        new_members = []
        for member in current_members:
            if member not in prev_member:
                new_members.append(member)

        events = []
        for member in dead_members:
            events.append(MembershipEvent(member, MEMBER_REMOVED, current_members))
            # TODO: Check this later
            """
            connection = self._client.connection_manager.get_connection(member)
            if connection is not None:
                connection.close("The client has closed the connection to this member,"
                                 " after receiving a member left event from the cluster. {}".format(connection))
            """

        for member in new_members:
            events.append(MembershipEvent(member, MEMBER_ADDED, current_members))

        if len(events) != 0:
            if len(self.member_list_snapshot) != 0:
                logging.info("MemberListSnapshot", extra=self._logger_extras)

        return events

    def fire_events(self, events):
        for event in events:
            for listener in self.listeners.values():
                if event.event_type == MEMBER_ADDED:
                    listener[0](event)
                else:
                    listener[1](event)

    def apply_initial_state(self, version, member_infos):
        snapshot = self.create_snapshot(version, member_infos)
        self.member_list_snapshot = snapshot
        # TODO: self._logger_extras has a class which is not printable
        # self.logger.info("MemberListSnapshot")
        members = snapshot.members.values()
        self._members = snapshot.members
        event = InitialMembershipEvent(members)

        for listener in self.listeners.values():
            if isinstance(listener, InitialMembershipEvent):
                listener.init(event)

    def create_snapshot(self, member_list_version, member_infos):
        new_members = {}
        for member_info in member_infos:
            member = MemberInfo(member_info.address, member_info.uuid, member_info.attributes, member_info.lite_member)
            new_members[member_info.uuid] = member

        return MemberListSnapshot(member_list_version, new_members)

    def clear_member_list_version(self):
        self.logger.debug("Resetting the member list version", extra=self._logger_extras)


class ClusterViewListenerService(object):
    logger = logging.getLogger("HazelcastClient.ClusterViewService")

    def __init__(self, client):
        self.client = client
        self._logger_extras = {"client_name": client.name, "cluster_name": client.config.cluster_name}
        self.connection_manager = client.connection_manager
        self.partition_service = client.partition_service
        self.cluster_service = client.cluster
        self.listener_added_connection = None
        self._lock = threading.Lock()

    def start(self):
        self.connection_manager.add_listener(on_connection_opened=self.connection_added,
                                             on_connection_closed=self.connection_removed)

    def connection_added(self, connection):
        self.try_register(connection)

    def connection_removed(self, connection, cause):
        self.try_reregister_to_random_connection(connection)
        pass

    def try_reregister_to_random_connection(self, old_connection):
        if self.listener_added_connection is not old_connection:
            # somebody else already trying to reregister
            return
        with self._lock:
            self.listener_added_connection = None

        new_connection = self.connection_manager.get_random_connection()
        # TODO: Check this one
        if new_connection is not None and new_connection.live():
            self.try_register(new_connection)

    def try_register(self, connection):
        if self.listener_added_connection is not None:
            # already registering/registered to another connection
            return
        with self._lock:
            self.listener_added_connection = connection

        client_message = client_add_cluster_view_listener_codec.encode_request()

        invocation = Invocation(self.client.invoker, client_message, connection=connection)

        handler = self.ClusterViewListenerHandler(connection, self)
        future = self.client.invoker.invoke_on_connection(client_message,
                                                          connection,
                                                          event_handler=
                                                          lambda m: client_add_cluster_view_listener_codec.handle(m,
        handle_partitions_view_event=lambda version, partitions: self.partition_service.handle_partitions_view_event(connection, partitions, version),
        handle_members_view_event=self.cluster_service.handle_members_view_event))
        invocation.event_handler = handler
        self.logger.debug("Register attempt of ClusterViewListenerHandler to " + repr(connection))

        def callback(f):
            try:
                f.result()
            except Exception as e:
                # print(str(e))
                self.try_reregister_to_random_connection(connection)

        # invocation.future.add_done_callback(callback)
        future.add_done_callback(callback)

    class ClusterViewListenerHandler:
        def __init__(self, connection, client_cluster_view_service):
            self.connection = connection
            self.client_cluster_view_service = client_cluster_view_service

        def on_listener_register(self, connection):
            ClusterViewListenerService.logger.debug("Registered ClusterViewListenerHandler to " + repr(connection))

        def handle_members_view_event(self, client_message):
            request = client_add_cluster_view_listener_codec.encode_request()
            self.client_cluster_view_service.client.listener.register_listener(
                request,
                lambda r: client_add_cluster_view_listener_codec.decode_response(r),
                lambda: None,
                lambda m: client_add_cluster_view_listener_codec.handle(m, handle_members_view_event=
                self.client_cluster_view_service.cluster_service.handle_members_view_event))

        def handle_partitions_view_event(self):
            request = client_add_cluster_view_listener_codec.encode_request()
            self.client_cluster_view_service.client.listener.register_listener(
                request,
                lambda r: client_add_cluster_view_listener_codec.decode_response(r),
                lambda: None,
                lambda m: client_add_cluster_view_listener_codec.handle(m,
                                                                        handle_partitions_view_event=self.client_cluster_view_service.partition_service.handle_partitions_view_event
                                                                        ,
                                                                        handle_members_view_event=self.client_cluster_view_service.cluster_service.handle_members_view_event))


class InitialMembershipEvent(object):
    """
    An event that is sent when a :class:`InitialMembershipListener` registers itself on a cluster. For more
    information, see the :class:`InitialMembershipListener`.

    .. seealso:: :class:`MembershipListener` and :class:`MembershipEvent`
    """

    # TODO: check if MembershipListener is necessary
    def __init__(self, members):
        self.members = members


class MembershipEvent(object):
    """
    Membership event fired when a new member is added to the cluster and/or when a member leaves the cluster
    or when there is a member attribute change.
    """

    member = None
    """The removed or added member."""

    event_type = None
    """The membership event type."""

    members = []
    """The members at the moment after this event."""

    def __init__(self, member, event_type, members):
        self.member = member
        self.event_type = event_type
        self.members = members


class MembershipListener(object):
    def member_added(self, membership):
        raise NotImplementedError

    def member_removed(self, membership):
        raise NotImplementedError


class AbstractLoadBalancer(MembershipListener):
    _members = None
    _cluster = None

    def next_address(self):
        raise NotImplementedError

    def init_load_balancer(self, cluster, config):
        self._cluster = cluster
        cluster.add_membership_listener(self)

    def init(self, event):
        self.set_members()

    def member_added(self, membership):
        self.set_members()

    def member_removed(self, membership):
        self.set_members()

    @property
    def members(self):
        return self._members

    def set_members(self):
        self._members = self._cluster.get_member_list()


class RandomLoadBalancer(AbstractLoadBalancer):
    """
    A LoadBalancer that selects a random member to route to.
    member.
    """

    def next_address(self):
        try:
            return random.choice(self._cluster.get_member_list())
        except IndexError:
            return None


class RoundRobinLoadBalancer(AbstractLoadBalancer):
    """
    RoundRobinLoadBalancer relies on using round robin to a next member to send a request to.
    """

    def __init__(self):
        self._index = random.randint(0, int(time.time()))

    def next_address(self):
        members = self._cluster.get_member_list()

        if members is None or len(members) == 0:
            return None

        length = len(members)
        self._index += 1
        index = self._index % length
        return members[index]


class VectorClock(object):
    """
    Vector clock consisting of distinct replica logical clocks.

    See https://en.wikipedia.org/wiki/Vector_clock
    The vector clock may be read from different thread but concurrent
    updates must be synchronized externally. There is no guarantee for
    concurrent updates.
    """

    def __init__(self):
        self._replica_timestamps = {}

    def is_after(self, other):
        """
        Returns true if this vector clock is causally strictly after the
        provided vector clock. This means that it the provided clock is neither
        equal to, greater than or concurrent to this vector clock.

        :param other: (:class:`~hazelcast.cluster.VectorClock`), Vector clock to be compared
        :return: (bool), True if this vector clock is strictly after the other vector clock, False otherwise
        """
        any_timestamp_greater = False
        for replica_id, other_timestamp in other.entry_set():
            local_timestamp = self._replica_timestamps.get(replica_id)

            if local_timestamp is None or local_timestamp < other_timestamp:
                return False
            elif local_timestamp > other_timestamp:
                any_timestamp_greater = True

        # there is at least one local timestamp greater or local vector clock has additional timestamps
        return any_timestamp_greater or other.size() < self.size()

    def set_replica_timestamp(self, replica_id, timestamp):
        """
        Sets the logical timestamp for the given replica ID.

        :param replica_id: (str), Replica ID.
        :param timestamp: (int), Timestamp for the given replica ID.
        """
        self._replica_timestamps[replica_id] = timestamp

    def entry_set(self):
        """
        Returns the entry set of the replica timestamps in a format
        of list of tuples. Each tuple contains the replica ID and the
        timestamp associated with it.

        :return: (list), List of tuples.
        """
        return list(self._replica_timestamps.items())

    def size(self):
        """
        Returns the number of timestamps that are in the
        replica timestamps dictionary.

        :return: (int), Number of timestamps in the replica timestamps.
        """
        return len(self._replica_timestamps)
