import logging
import random
import threading
import time
from hazelcast.core import CLIENT_TYPE, SERIALIZATION_VERSION, Address
# Membership Event Types
from hazelcast.exception import HazelcastError, AuthenticationError
from hazelcast.invocation import ListenerInvocation
from hazelcast.protocol.codec import client_add_membership_listener_codec, client_authentication_codec

MEMBER_ADDED = 1
MEMBER_REMOVED = 2


class ClusterService(object):
    logger = logging.getLogger("ClusterService")

    def __init__(self, config, client):
        self._config = config
        self._client = client
        self.member_list = []
        self.owner_connection_address = None
        self.owner_uuid = None
        self.uuid = None
        self._initial_list_fetched = threading.Event()
        self._client.connection_manager.add_listener(on_connection_closed=self._connection_closed)

    def start(self):
        self._connect_to_cluster()

    def size(self):
        return len(self.member_list)

    @staticmethod
    def _parse_addr(addr):
        (host, port) = addr.split(":")
        return Address(host, int(port))

    def _reconnect(self):
        try:
            self.logger.warn("Connection closed to owner node. Trying to reconnect.")
            self._connect_to_cluster()
        except:
            logging.exception("Could not reconnect to cluster. Shutting down client.")
            self._client.shutdown()

    def _connect_to_cluster(self):
        address = self._parse_addr(self._config.network_config.addresses[0])  # TODO: try all addresses
        self.logger.info("Connecting to %s", address)

        current_attempt = 1
        attempt_limit = self._config.network_config.connection_attempt_limit
        retry_delay = self._config.network_config.connection_attempt_period / 1000
        while current_attempt <= self._config.network_config.connection_attempt_limit:
            try:
                self._connect_to_address(address)
                return
            except:
                self.logger.warning("Error connecting to %s, attempt %d of %d, trying again in %d seconds",
                                    address, current_attempt, attempt_limit, retry_delay, exc_info=True)
                time.sleep(retry_delay)
                current_attempt += 1

        raise HazelcastError("Could not connect to the given addresses after %d tries" % (attempt_limit,))

    def _authenticate_manager(self, connection):
        request = client_authentication_codec.encode_request(
            username=self._config.group_config.name, password=self._config.group_config.password,
            uuid=None, owner_uuid=None, is_owner_connection=True, client_type=CLIENT_TYPE,
            serialization_version=SERIALIZATION_VERSION)

        def callback(f):
            if f.is_success():
                parameters = client_authentication_codec.decode_response(f.result())
                if parameters["status"] != 0:
                    raise AuthenticationError("Authentication failed.")
                connection.endpoint = parameters["address"]
                self.owner_uuid = parameters["owner_uuid"]
                self.uuid = parameters["uuid"]
                return connection
            else:
                raise f.exception()

        return self._client.invoker.invoke_on_connection(request, connection).continue_with(callback)

    def _connect_to_address(self, address):
        connection = self._client.connection_manager.get_or_connect(address, self._authenticate_manager).result()
        self.owner_connection_address = connection.endpoint
        self._init_membership_listener(connection)

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
            self.member_list.append(member)
        elif event_type == MEMBER_REMOVED:
            self.member_list.remove(member)
            # TODO, check owner connection, destroy connection

        self._log_member_list()
        self._client.partition_service.refresh()

    def _handle_member_list(self, members):
        self.logger.debug("Got member list")
        self.member_list = members
        self._log_member_list()
        self._client.partition_service.refresh()
        self._initial_list_fetched.set()

    def _log_member_list(self):
        self.logger.info("New member list:\n\nMembers [%d] {\n%s\n}\n", len(self.member_list),
                         "\n".join(["\t" + str(x) for x in self.member_list]))

    def _connection_closed(self, connection):
        if connection.endpoint and connection.endpoint == self.owner_connection_address:
            # try to reconnect, on new thread
            reconnect_thread = threading.Thread(target=self._reconnect, name="hazelcast-cluster-reconnect")
            reconnect_thread.daemon = True
            reconnect_thread.start()

    def shutdown(self):
        pass


class RandomLoadBalancer(object):
    def __init__(self, cluster):
        self._cluster = cluster

    def next_address(self):
        return random.choice(self._cluster.member_list).address
