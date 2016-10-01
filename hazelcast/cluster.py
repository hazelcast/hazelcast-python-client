import logging
import random
import threading
import time
import uuid

from hazelcast.core import CLIENT_TYPE, SERIALIZATION_VERSION
from hazelcast.exception import HazelcastError, AuthenticationError, TargetDisconnectedError
from hazelcast.invocation import ListenerInvocation
from hazelcast.lifecycle import LIFECYCLE_STATE_CONNECTED, LIFECYCLE_STATE_DISCONNECTED
from hazelcast.protocol.codec import client_add_membership_listener_codec, client_authentication_codec
from hazelcast.util import get_possible_addresses

# Membership Event Types
MEMBER_ADDED = 1
MEMBER_REMOVED = 2


class ClusterService(object):
    """
    Hazelcast cluster service. It provides access to the members in the cluster and the client can register for changes
    in the cluster members.

    All the methods on the Cluster are thread-safe.
    """
    logger = logging.getLogger("ClusterService")

    def __init__(self, config, client):
        self._config = config
        self._client = client
        self.members = []
        self.owner_connection_address = None
        self.owner_uuid = None
        self.uuid = None
        self.listeners = {}

        for listener in config.membership_listeners:
            self.add_listener(*listener)

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
        return len(self.members)

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
            for member in self.members:
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

    def _reconnect(self):
        try:
            self.logger.warn("Connection closed to owner node. Trying to reconnect.")
            self._connect_to_cluster()
        except:
            logging.exception("Could not reconnect to cluster. Shutting down client.")
            self._client.shutdown()

    def _connect_to_cluster(self):
        addresses = get_possible_addresses(self._config.network_config.addresses, self.members)

        current_attempt = 1
        attempt_limit = self._config.network_config.connection_attempt_limit
        retry_delay = self._config.network_config.connection_attempt_period
        while current_attempt <= attempt_limit:
            for address in addresses:
                try:
                    if current_attempt > attempt_limit:
                        break
                    self.logger.info("Connecting to %s", address)
                    self._connect_to_address(address)
                    return
                except:
                    self.logger.warning("Error connecting to %s, attempt %d of %d, trying again in %d seconds",
                                        address, current_attempt, attempt_limit, retry_delay, exc_info=True)
                    time.sleep(retry_delay)
                current_attempt += 1

        error_msg = "Could not connect to any of %s after %d tries" % (addresses, attempt_limit)
        raise HazelcastError(error_msg)

    def _authenticate_manager(self, connection):
        request = client_authentication_codec.encode_request(
            username=self._config.group_config.name, password=self._config.group_config.password,
            uuid=None, owner_uuid=None, is_owner_connection=True, client_type=CLIENT_TYPE,
            serialization_version=SERIALIZATION_VERSION)

        def callback(f):
            parameters = client_authentication_codec.decode_response(f.result())
            if parameters["status"] != 0:  # TODO: handle other statuses
                raise AuthenticationError("Authentication failed.")
            connection.endpoint = parameters["address"]
            connection.is_owner = True
            self.owner_uuid = parameters["owner_uuid"]
            self.uuid = parameters["uuid"]
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
            ListenerInvocation(request, handler, connection=connection)).result()
        registration_id = client_add_membership_listener_codec.decode_response(response)["response"]
        self.logger.debug("Registered membership listener with ID " + registration_id)
        self._initial_list_fetched.wait()

    def _handle_member(self, member, event_type):
        self.logger.debug("Got member event: %s, %s", member, event_type)
        if event_type == MEMBER_ADDED:
            self._member_added(member)
        elif event_type == MEMBER_REMOVED:
            self._member_removed(member)

        self._log_member_list()
        self._client.partition_service.refresh()

    def _handle_member_list(self, members):
        self.logger.debug("Got initial member list: %s", members)

        for m in list(self.members):
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
        self.members.append(member)
        for added, _ in self.listeners.values():
            if added:
                try:
                    added(member)
                except:
                    logging.exception("Exception in membership listener")

    def _member_removed(self, member):
        self.members.remove(member)
        self._client.connection_manager.close_connection(member.address, TargetDisconnectedError(
            "%s is no longer a member of the cluster" % member))
        for _, removed in self.listeners.values():
            if removed:
                try:
                    removed(member)
                except:
                    logging.exception("Exception in membership listener")

    def _log_member_list(self):
        self.logger.info("New member list:\n\nMembers [%d] {\n%s\n}\n", len(self.members),
                         "\n".join(["\t" + str(x) for x in self.members]))

    def _connection_closed(self, connection, _):
        if connection.endpoint and connection.endpoint == self.owner_connection_address \
                and self._client.lifecycle.is_live:
            self._client.lifecycle.fire_lifecycle_event(LIFECYCLE_STATE_DISCONNECTED)
            self.owner_connection_address = None
            # clear member list as owner connection is lost
            self.members = []

            # try to reconnect, on new thread
            reconnect_thread = threading.Thread(target=self._reconnect,
                                                name="hazelcast-cluster-reconnect-{:.4}".format(uuid.uuid4()))
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
        :return: (Member), the corresponding member.
        """
        for member in self.members:
            if member.uuid == member_uuid:
                return member


class RandomLoadBalancer(object):
    """
    RandomLoadBalancer make the Client send operations randomly on members not to increase the load on a specific
    member.
    """
    def __init__(self, cluster):
        self._cluster = cluster

    def next_address(self):
        try:
            return random.choice(self._cluster.members).address
        except IndexError:
            return None
