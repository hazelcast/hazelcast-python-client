import logging
import random
import threading

from hazelcast.core import CLIENT_TYPE, SERIALIZATION_VERSION, Address

# Membership Event Types
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

    def start(self):
        self._connect_to_cluster()

    def size(self):
        return len(self.member_list)

    @staticmethod
    def _parse_addr(addr):
        (host, port) = addr.split(":")
        return Address(host, int(port))

    def _connect_to_cluster(self):
        address = self._parse_addr(self._config.network_config.addresses[0])
        self.logger.info("Connecting to %s", address)

        def authenticate_manager(conn):
            request = client_authentication_codec.encode_request(
                username=self._config.group_config.name, password=self._config.group_config.password,
                uuid=None, owner_uuid=None, is_owner_connection=True, client_type=CLIENT_TYPE,
                serialization_version=SERIALIZATION_VERSION)
            response = self._client.invoker.invoke_on_connection(request, conn).future.result()
            parameters = client_authentication_codec.decode_response(response)
            if parameters["status"] != 0:
                raise RuntimeError("Authentication failed")
            conn.endpoint = parameters["address"]
            self.owner_uuid = parameters["owner_uuid"]
            self.uuid = parameters["uuid"]

        connection = self._client.connection_manager.get_or_connect(address, authenticate_manager)

        self.owner_connection_address = connection.endpoint
        self._init_membership_listener(connection)

    def _init_membership_listener(self, connection):
        request = client_add_membership_listener_codec.encode_request(False)

        def handler(m):
            client_add_membership_listener_codec.handle(m, self._handle_member, self._handle_member_list)

        response = self._client.invoker.invoke_on_connection(request, connection, handler).future.result()
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

        self.logger.info("New member list is: %s", self.member_list)
        self._client.partition_service.refresh()

    def _handle_member_list(self, member_list):
        self.logger.debug("Got member list")
        self.member_list = member_list
        self.logger.info("New member list is: %s", member_list)
        self._client.partition_service.refresh()
        self._initial_list_fetched.set()

    def shutdown(self):
        pass

class RandomLoadBalancer(object):
    def __init__(self, cluster):
        self._cluster = cluster

    def next_address(self):
        return random.choice(self._cluster.member_list).address
