import logging
import random
import threading

from hazelcast.codec import client_authentication_codec, \
    client_add_membership_listener_codec, \
    client_get_partitions_codec
from hazelcast.core import CLIENT_TYPE, SERIALIZATION_VERSION

# Membership Event Types
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

    def start(self):
        self.connect_to_cluster()

    @staticmethod
    def _parse_addr(addr):
        (host, port) = addr.split(":")
        return host, int(port)

    def connect_to_cluster(self):
        address = self._parse_addr(self._config.addresses[0])
        self.logger.info("Connecting to address %s", address)

        def authenticate_manager(conn):
            request = client_authentication_codec.encode_request(
                username=self._config.username, password=self._config.password,
                uuid=None, owner_uuid=None, is_owner_connection=True, client_type=CLIENT_TYPE,
                serialization_version=SERIALIZATION_VERSION)
            response = self._client.invoker.invoke_on_connection(request, conn).result()
            parameters = client_authentication_codec.decode_response(response)
            if parameters["status"] != 0:
                raise RuntimeError("Authentication failed")
            conn.endpoint = parameters["address"]
            self.owner_uuid = parameters["owner_uuid"]
            self.uuid = parameters["uuid"]

        connection = self._client.connection_manager.get_or_connect(address, authenticate_manager)

        self.owner_connection_address = connection.endpoint
        self.init_membership_listener(connection)

    def init_membership_listener(self, connection):
        request = client_add_membership_listener_codec.encode_request(False)

        def handler(m):
            client_add_membership_listener_codec.handle(m, self.handle_member, self.handle_member_list)

        response = self._client.invoker.invoke_on_connection(request, connection, handler).result()
        registration_id = client_add_membership_listener_codec.decode_response(response)["response"]
        self.logger.debug("Registered membership listener with ID " + registration_id)

    def handle_member(self, member, event_type):
        self.logger.debug("Got member event: %s, %s", member, event_type)
        if event_type == MEMBER_ADDED:
            self.member_list.append(member)
        elif event_type == MEMBER_REMOVED:
            self.member_list.remove(member)
            # TODO, check owner connection, destroy connection

        self.logger.info("New member list is: %s", self.member_list)
        self._client.partition_service.refresh()

    def handle_member_list(self, member_list):
        self.logger.debug("Got member list")
        self.member_list = member_list
        self.logger.info("New member list is: %s", member_list)
        self._client.partition_service.refresh()




class RandomLoadBalancer(object):
    def __init__(self, cluster):
        self._cluster = cluster

    def next_address(self):
        return random.choice(self._cluster.member_list()).address
