import logging
import random
import threading
import time
import uuid

from hazelcast.exception import HazelcastError, AuthenticationError, TargetDisconnectedError
from hazelcast.invocation import ListenerInvocation
from hazelcast.lifecycle import LIFECYCLE_STATE_CONNECTED, LIFECYCLE_STATE_DISCONNECTED
from hazelcast.protocol.codec import client_add_membership_listener_codec, client_authentication_codec
from hazelcast.util import get_possible_addresses, get_provider_addresses, calculate_version
from hazelcast.version import CLIENT_TYPE, CLIENT_VERSION, SERIALIZATION_VERSION

# Membership Event Types
MEMBER_ADDED = 1
MEMBER_REMOVED = 2


class ClusterService(object):
    """
    Hazelcast cluster service. It provides access to the members in the cluster and the client can register for changes
    in the cluster members.

    All the methods on the Cluster are thread-safe.
    """
    logger = logging.getLogger("HazelcastClient.ClusterService")

    def __init__(self, config, client, address_providers):
        self._config = config
        self._client = client
        self._logger_extras = {"client_name": client.name, "group_name": config.group_config.name}
        self._members = {}
        self.owner_connection_address = None
        self.owner_uuid = None
        self.uuid = None
        self.listeners = {}

        for listener in config.membership_listeners:
            self.add_listener(*listener)

        self._address_providers = address_providers
        self._initial_list_fetched = threading.Event()
        self._client.connection_manager.add_listener(on_connection_closed=self._connection_closed)
        self._client.heartbeat.add_listener(on_heartbeat_stopped=self._heartbeat_stopped)

    def start(self):
        """
        Connects to cluster.
        """
        self._connect_to_cluster()

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

    def _reconnect(self):
        try:
            self.logger.warning("Connection closed to owner node. Trying to reconnect.", extra=self._logger_extras)
            self._connect_to_cluster()
        except:
            self.logger.exception("Could not reconnect to cluster. Shutting down client.", extra=self._logger_extras)
            self._client.shutdown()

    def _connect_to_cluster(self):
        current_attempt = 1
        attempt_limit = self._config.network_config.connection_attempt_limit
        retry_delay = self._config.network_config.connection_attempt_period
        while current_attempt <= attempt_limit:
            provider_addresses = get_provider_addresses(self._address_providers)
            addresses = get_possible_addresses(provider_addresses, self.get_member_list())

            for address in addresses:
                try:
                    self.logger.info("Connecting to %s", address, extra=self._logger_extras)
                    self._connect_to_address(address)
                    return
                except:
                    self.logger.warning("Error connecting to %s ", address, exc_info=True, extra=self._logger_extras)

            if current_attempt >= attempt_limit:
                self.logger.warning(
                    "Unable to get alive cluster connection, attempt %d of %d",
                    current_attempt, attempt_limit, extra=self._logger_extras)
                break

            self.logger.warning(
                "Unable to get alive cluster connection, attempt %d of %d, trying again in %d seconds",
                current_attempt, attempt_limit, retry_delay, extra=self._logger_extras)
            current_attempt += 1
            time.sleep(retry_delay)

        error_msg = "Could not connect to any of %s after %d tries" % (addresses, attempt_limit)
        raise HazelcastError(error_msg)

    def _authenticate_manager(self, connection):
        request = client_authentication_codec.encode_request(
            username=self._config.group_config.name,
            password=self._config.group_config.password,
            uuid=self.uuid,
            owner_uuid=self.owner_uuid,
            is_owner_connection=True,
            client_type=CLIENT_TYPE,
            serialization_version=SERIALIZATION_VERSION,
            client_hazelcast_version=CLIENT_VERSION)

        def callback(f):
            parameters = client_authentication_codec.decode_response(f.result())
            if parameters["status"] != 0:  # TODO: handle other statuses
                raise AuthenticationError("Authentication failed.")
            connection.endpoint = parameters["address"]
            connection.is_owner = True
            self.owner_uuid = parameters["owner_uuid"]
            self.uuid = parameters["uuid"]
            connection.server_version_str = parameters.get("server_hazelcast_version", "")
            connection.server_version = calculate_version(connection.server_version_str)
            return connection

        return self._client.invoker.invoke_on_connection(request, connection).continue_with(callback)

    def _connect_to_address(self, address):
        f = self._client.connection_manager.get_or_connect(address, self._authenticate_manager)
        connection = f.result()
        if not connection.is_owner:
            self._authenticate_manager(connection).result()
        self.owner_connection_address = connection.endpoint
        self._init_membership_listener(connection)
        self._client.lifecycle.fire_lifecycle_event(LIFECYCLE_STATE_CONNECTED)

    def _init_membership_listener(self, connection):
        request = client_add_membership_listener_codec.encode_request(False)

        def handler(m):
            client_add_membership_listener_codec.handle(m, self._handle_member, self._handle_member_list)

        response = self._client.invoker.invoke(
            ListenerInvocation(self._client.listener, request, handler, connection=connection)).result()
        registration_id = client_add_membership_listener_codec.decode_response(response)["response"]
        self.logger.debug("Registered membership listener with ID " + registration_id, extra=self._logger_extras)
        self._initial_list_fetched.wait()

    def _handle_member(self, member, event_type):
        self.logger.debug("Got member event: %s, %s", member, event_type, extra=self._logger_extras)
        if event_type == MEMBER_ADDED:
            self._member_added(member)
        elif event_type == MEMBER_REMOVED:
            self._member_removed(member)

        self._log_member_list()
        self._client.partition_service.refresh()

    def _handle_member_list(self, members):
        self.logger.debug("Got initial member list: %s", members, extra=self._logger_extras)

        for m in self.get_member_list():
            try:
                members.remove(m)
            except ValueError:
                self._member_removed(m)
        for m in members:
            self._member_added(m)

        self._log_member_list()
        self._client.partition_service.refresh()
        self._initial_list_fetched.set()

    def _member_added(self, member):
        self._members[member.address] = member
        for added, _ in list(self.listeners.values()):
            if added:
                try:
                    added(member)
                except:
                    self.logger.exception("Exception in membership listener", extra=self._logger_extras)

    def _member_removed(self, member):
        self._members.pop(member.address, None)
        self._client.connection_manager.close_connection(member.address, TargetDisconnectedError(
            "%s is no longer a member of the cluster" % member))
        for _, removed in list(self.listeners.values()):
            if removed:
                try:
                    removed(member)
                except:
                    self.logger.exception("Exception in membership listener", extra=self._logger_extras)

    def _log_member_list(self):
        self.logger.info("New member list:\n\nMembers [%d] {\n%s\n}\n", self.size(),
                         "\n".join(["\t" + str(x) for x in self.get_member_list()]), extra=self._logger_extras)

    def _connection_closed(self, connection, _):
        if connection.endpoint and connection.endpoint == self.owner_connection_address \
                and self._client.lifecycle.is_live:
            self._client.lifecycle.fire_lifecycle_event(LIFECYCLE_STATE_DISCONNECTED)
            self.owner_connection_address = None

            # try to reconnect, on new thread
            reconnect_thread = threading.Thread(target=self._reconnect,
                                                name="hazelcast-cluster-reconnect-{:.4}".format(str(uuid.uuid4())))
            reconnect_thread.daemon = True
            reconnect_thread.start()

    def _heartbeat_stopped(self, connection):
        if connection.endpoint == self.owner_connection_address:
            self._client.connection_manager.close_connection(connection.endpoint, TargetDisconnectedError(
                "%s stopped heart beating." % connection))

    def get_member_by_uuid(self, member_uuid):
        """
        Returns the member with specified member uuid.

        :param member_uuid: (int), uuid of the desired member.
        :return: (:class:`~hazelcast.core.Member`), the corresponding member.
        """
        for member in self.get_member_list():
            if member.uuid == member_uuid:
                return member

    def get_member_by_address(self, address):
        """
        Returns the member with the specified address if it is in the
        cluster, None otherwise.

        :param address: (:class:`~hazelcast.core.Address`), address of the desired member.
        :return: (:class:`~hazelcast.core.Member`), the corresponding member.
        """
        return self._members.get(address, None)

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
