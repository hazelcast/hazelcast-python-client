import logging
import threading
import uuid
from collections import OrderedDict

from hazelcast import six
from hazelcast.errors import TargetDisconnectedError, IllegalStateError
from hazelcast.util import check_not_none

_logger = logging.getLogger(__name__)


class _MemberListSnapshot(object):
    __slots__ = ("version", "members")

    def __init__(self, version, members):
        self.version = version
        self.members = members


class ClientInfo(object):
    """Local information of the client.

    Attributes:
        uuid (uuid.UUID): Unique id of this client instance.
        address (hazelcast.core.Address): Local address that is used to communicate with cluster.
        name (str): Name of the client.
        labels (set[str]): Read-only set of all labels of this client.
    """

    __slots__ = ("uuid", "address", "name", "labels")

    def __init__(self, client_uuid, address, name, labels):
        self.uuid = client_uuid
        self.address = address
        self.name = name
        self.labels = labels

    def __repr__(self):
        return "ClientInfo(uuid=%s, address=%s, name=%s, labels=%s)" % (
            self.uuid,
            self.address,
            self.name,
            self.labels,
        )


_EMPTY_SNAPSHOT = _MemberListSnapshot(-1, OrderedDict())
_INITIAL_MEMBERS_TIMEOUT_SECONDS = 120


class ClusterService(object):
    """
    Cluster service for Hazelcast clients.

    It provides access to the members in the cluster
    and one can register for changes in the cluster members.
    """

    def __init__(self, internal_cluster_service):
        self._service = internal_cluster_service

    def add_listener(self, member_added=None, member_removed=None, fire_for_existing=False):
        """
        Adds a membership listener to listen for membership updates.

        It will be notified when a member is added to cluster or removed from cluster.
        There is no check for duplicate registrations, so if you register the listener
        twice, it will get events twice.

        Args:
            member_added (function): Function to be called when a member is added to the cluster.
            member_removed (function): Function to be called when a member is removed from the cluster.
            fire_for_existing (bool): Whether or not fire member_added for existing members.

        Returns:
            str: Registration id of the listener which will be used for removing this listener.
        """
        return self._service.add_listener(member_added, member_removed, fire_for_existing)

    def remove_listener(self, registration_id):
        """
        Removes the specified membership listener.

        Args:
            registration_id (str): Registration id of the listener to be removed.

        Returns:
            bool: ``True`` if the registration is removed, ``False`` otherwise.
        """
        return self._service.remove_listener(registration_id)

    def get_members(self, member_selector=None):
        """
        Lists the current members in the cluster.

        Every member in the cluster returns the members in the same order.
        To obtain the oldest member in the cluster, you can retrieve the first item in the list.

        Args:
            member_selector (function): Function to filter members to return.
                If not provided, the returned list will contain all the available cluster members.

        Returns:
            list[hazelcast.core.MemberInfo]: Current members in the cluster
        """
        return self._service.get_members(member_selector)


class _InternalClusterService(object):
    def __init__(self, client, config):
        self._client = client
        self._connection_manager = None
        self._labels = frozenset(config.labels)
        self._listeners = {}
        self._member_list_snapshot = _EMPTY_SNAPSHOT
        self._initial_list_fetched = threading.Event()

    def start(self, connection_manager, membership_listeners):
        self._connection_manager = connection_manager
        for listener in membership_listeners:
            self.add_listener(*listener)

    def get_member(self, member_uuid):
        check_not_none(uuid, "UUID must not be null")
        snapshot = self._member_list_snapshot
        return snapshot.members.get(member_uuid, None)

    def get_members(self, member_selector=None):
        snapshot = self._member_list_snapshot
        if not member_selector:
            return list(snapshot.members.values())

        members = []
        for member in six.itervalues(snapshot.members):
            if member_selector(member):
                members.append(member)
        return members

    def size(self):
        """
        Returns:
            int: Size of the cluster.
        """
        snapshot = self._member_list_snapshot
        return len(snapshot.members)

    def get_local_client(self):
        """
        Returns:
            hazelcast.cluster.ClientInfo: The client info.
        """
        connection_manager = self._connection_manager
        connection = connection_manager.get_random_connection()
        local_address = None if not connection else connection.local_address
        return ClientInfo(
            connection_manager.client_uuid, local_address, self._client.name, self._labels
        )

    def add_listener(self, member_added=None, member_removed=None, fire_for_existing=False):
        registration_id = str(uuid.uuid4())
        self._listeners[registration_id] = (member_added, member_removed)

        if fire_for_existing and member_added:
            snapshot = self._member_list_snapshot
            for member in six.itervalues(snapshot.members):
                member_added(member)

        return registration_id

    def remove_listener(self, registration_id):
        try:
            self._listeners.pop(registration_id)
            return True
        except KeyError:
            return False

    def wait_initial_member_list_fetched(self):
        """Blocks until the initial member list is fetched from the cluster.

        If it is not received within the timeout, an error is raised.

        Raises:
            IllegalStateError: If the member list could not be fetched
        """
        fetched = self._initial_list_fetched.wait(_INITIAL_MEMBERS_TIMEOUT_SECONDS)
        if not fetched:
            raise IllegalStateError("Could not get initial member list from cluster!")

    def clear_member_list_version(self):
        _logger.debug("Resetting the member list version")

        current = self._member_list_snapshot
        if current is not _EMPTY_SNAPSHOT:
            self._member_list_snapshot = _MemberListSnapshot(0, current.members)

    def handle_members_view_event(self, version, member_infos):
        snapshot = self._create_snapshot(version, member_infos)
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Handling new snapshot with membership version: %s, member string: %s",
                version,
                self._members_string(snapshot),
            )

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
                    try:
                        handler(removed_member)
                    except:
                        _logger.exception("Exception in membership listener")

        for added_member in additions:
            for handler, _ in six.itervalues(self._listeners):
                if handler:
                    try:
                        handler(added_member)
                    except:
                        _logger.exception("Exception in membership listener")

    def _detect_membership_events(self, old, new):
        new_members = []
        dead_members = set(six.itervalues(old.members))
        for member in six.itervalues(new.members):
            try:
                dead_members.remove(member)
            except KeyError:
                new_members.append(member)

        for dead_member in dead_members:
            connection = self._connection_manager.get_connection(dead_member.uuid)
            if connection:
                connection.close(
                    None,
                    TargetDisconnectedError(
                        "The client has closed the connection to this member, "
                        "after receiving a member left event from the cluster. "
                        "%s" % connection
                    ),
                )

        if (len(new_members) + len(dead_members)) > 0:
            if len(new.members) > 0:
                _logger.info(self._members_string(new))

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


class VectorClock(object):
    """Vector clock consisting of distinct replica logical clocks.

    The vector clock may be read from different thread but concurrent
    updates must be synchronized externally. There is no guarantee for
    concurrent updates.

    See Also:
        https://en.wikipedia.org/wiki/Vector_clock
    """

    def __init__(self):
        self._replica_timestamps = {}

    def is_after(self, other):
        """Returns ``True`` if this vector clock is causally strictly after the
        provided vector clock. This means that it the provided clock is neither
        equal to, greater than or concurrent to this vector clock.

        Args:
            other (hazelcast.cluster.VectorClock): Vector clock to be compared

        Returns:
            bool: ``True`` if this vector clock is strictly after the other vector clock, ``False`` otherwise.
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
        """Sets the logical timestamp for the given replica ID.

        Args:
            replica_id (str): Replica ID.
            timestamp (int): Timestamp for the given replica ID.
        """
        self._replica_timestamps[replica_id] = timestamp

    def entry_set(self):
        """Returns the entry set of the replica timestamps in a format of list of tuples.

        Each tuple contains the replica ID and the timestamp associated with it.

        Returns:
            list: List of tuples.
        """
        return list(self._replica_timestamps.items())

    def size(self):
        """Returns the number of timestamps that are in the replica timestamps dictionary.

        Returns:
            int: Number of timestamps in the replica timestamps.
        """
        return len(self._replica_timestamps)
