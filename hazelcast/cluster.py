import logging
import random
import threading
import uuid
from collections import OrderedDict

from hazelcast import six
from hazelcast.exception import TargetDisconnectedError, IllegalStateError
from hazelcast.util import check_not_none

# Membership Event Types
MEMBER_ADDED = 1
MEMBER_REMOVED = 2


class _MemberListSnapshot(object):
    __slots__ = ("version", "members")

    def __init__(self, version, members):
        self.version = version
        self.members = members


class ClientInfo(object):
    __slots__ = ("uuid", "address", "name", "labels")

    def __init__(self, client_uuid, address, name, labels):
        self.uuid = client_uuid
        """Unique id of this client instance."""

        self.address = address
        """Local address that is used to communicate with cluster."""

        self.name = name
        """Name of the client."""

        self.labels = labels
        """Read-only set of all labels of this client."""

    def __repr__(self):
        return "ClientInfo(uuid=%s, address=%s, name=%s, labels=%s)" % (self.uuid, self.address, self.name, self.labels)


_EMPTY_SNAPSHOT = _MemberListSnapshot(-1, OrderedDict())
_INITIAL_MEMBERS_TIMEOUT_SECONDS = 120


class ClusterService(object):
    """Cluster service for Hazelcast clients.
    Allows to retrieve Hazelcast members of the cluster, e.g. by their Address or UUID.
    """
    logger = logging.getLogger("HazelcastClient.ClusterService")

    def __init__(self, client):
        self._client = client
        config = client.config
        self._logger_extras = {"client_name": client.name, "cluster_name": config.cluster_name}
        self._labels = frozenset(config.labels)
        self._conn_manager = client.connection_manager
        self._listeners = {}
        self._member_list_snapshot = _EMPTY_SNAPSHOT
        self._initial_list_fetched = threading.Event()

    def get_member(self, member_uuid):
        """Gets the member with the given UUID.

        :param member_uuid: (:class: `~uuid.UUID`), uuid of the desired member.
        :return: (:class:`~hazelcast.core.Member`), the corresponding member.
        """
        check_not_none(uuid, "UUID must not be null")
        snapshot = self._member_list_snapshot
        return snapshot.members.get(member_uuid, None)

    def get_members(self, selector=None):
        """
        Returns the members that satisfy the given selector if any.

        :param selector: (Function), Selector to be applied to the members.
        :return: (List), List of members.
        """
        snapshot = self._member_list_snapshot
        if not selector:
            return list(snapshot.members.values())

        members = []
        for member in six.itervalues(snapshot.members):
            if selector(member):
                members.append(member)
        return members

    def size(self):
        """
        Returns the size of the cluster.

        :return: (int), size of the cluster.
        """
        snapshot = self._member_list_snapshot
        return len(snapshot.members)

    def get_local_client(self):
        """
        Returns the info representing the local client.

        :return: (:class: `~hazelcast.cluster.ClientInfo`), client info
        """
        conn_manager = self._conn_manager
        conn = conn_manager.get_random_connection()
        local_addr = None if not conn else conn.get_local_address()
        return ClientInfo(conn_manager.get_client_uuid(), local_addr, self._client.name, self._labels)

    def add_listener(self, member_added=None, member_removed=None, fire_for_existing=False):
        """
        Adds a membership listener to listen for membership updates, it will be notified when a member is added to
        cluster or removed from cluster. There is no check for duplicate registrations, so if you register the listener
        twice, it will get events twice.


        :param member_added: (Function), function to be called when a member is added to the cluster (optional).
        :param member_removed: (Function), function to be called when a member is removed to the cluster (optional).
        :param fire_for_existing: (bool), whether or not fire member_added for existing members (optional).
        :return: (str), registration id of the listener which will be used for removing this listener.
        """
        registration_id = str(uuid.uuid4())
        self._listeners[registration_id] = (member_added, member_removed)

        if fire_for_existing and member_added:
            snapshot = self._member_list_snapshot
            for member in six.itervalues(snapshot.members):
                member_added(member)

        return registration_id

    def remove_listener(self, registration_id):
        """
        Removes the specified membership listener.

        :param registration_id: (str), registration id of the listener to be deleted.
        :return: (bool), if the registration is removed, ``false`` otherwise.
        """
        try:
            self._listeners.pop(registration_id)
            return True
        except KeyError:
            return False

    def wait_initial_member_list_fetched(self):
        """
        Blocks until the initial member list is fetched from the cluster.
        If it is not received within the timeout, an error is raised.

        :raises IllegalStateError: If the member list could not be fetched
        """
        fetched = self._initial_list_fetched.wait(_INITIAL_MEMBERS_TIMEOUT_SECONDS)
        if not fetched:
            raise IllegalStateError("Could not get initial member list from cluster!")

    def clear_member_list_version(self):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Resetting the member list version", extra=self._logger_extras)

        current = self._member_list_snapshot
        if current is not _EMPTY_SNAPSHOT:
            self._member_list_snapshot = _MemberListSnapshot(0, current.members)

    def handle_members_view_event(self, version, member_infos):
        snapshot = self._create_snapshot(version, member_infos)
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Handling new snapshot with membership version: %s, member string: %s"
                              % (version, self._members_string(snapshot)), extra=self._logger_extras)

        current = self._member_list_snapshot
        if version >= current.version:
            self._apply_new_state_and_fire_events(current, snapshot)

        if current is _EMPTY_SNAPSHOT:
            self._initial_list_fetched.set()

    def _apply_new_state_and_fire_events(self, current, snapshot):
        self._member_list_snapshot = snapshot
        removals, additions = self._detect_membership_events(current, snapshot)

        # Removal events should be fired first
        for removed_member in removals:
            for _, handler in six.itervalues(self._listeners):
                if handler:
                    handler(removed_member)

        for added_member in additions:
            for handler, _ in six.itervalues(self._listeners):
                if handler:
                    handler(added_member)

    def _detect_membership_events(self, old, new):
        new_members = []
        dead_members = set(six.itervalues(old.members))
        for member in six.itervalues(new.members):
            try:
                dead_members.remove(member)
            except KeyError:
                new_members.append(member)

        for dead_member in dead_members:
            conn = self._conn_manager.get_connection(dead_member.uuid)
            if conn:
                conn.close(None, TargetDisconnectedError("The client has closed the connection to this member, after "
                                                         "receiving a member left event from the cluster. %s" % conn))

        if (len(new_members) + len(dead_members)) > 0:
            if len(new.members) > 0:
                self.logger.info(self._members_string(new), extra=self._logger_extras)

        return dead_members, new_members

    @staticmethod
    def _members_string(snapshot):
        members = snapshot.members
        n = len(members)
        return "\n\nMembers [%s] {\n\t%s\n}\n" % (n, "\n\t".join(map(str, six.itervalues(members))))

    @staticmethod
    def _create_snapshot(version, member_infos):
        new_members = OrderedDict()
        for member_info in member_infos:
            new_members[member_info.uuid] = member_info
        return _MemberListSnapshot(version, new_members)


class RandomLoadBalancer(object):
    """
    RandomLoadBalancer make the Client send operations randomly on members not to increase the load on a specific
    member.
    """

    def __init__(self, cluster):
        self._cluster = cluster

    def next_address(self):
        try:
            return random.choice(self._cluster.get_member_list()).address
        except IndexError:
            return None


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
