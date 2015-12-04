import logging
import threading

from hazelcast.codec import client_authentication_codec, \
    client_add_membership_listener_codec, \
    client_get_partitions_codec


class ClusterService(object):

    logger = logging.getLogger("ClusterService")

    def __init__(self, config, client):
        self._config = config
        self._client = client
        self.member_list = []
        self.owner_connection_address = None

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
                self._config.username, self._config.password, None, None, True, "PHY", 1)
            response = self._client.invoker.invoke_on_connection(request, conn).result()
            parameters = client_authentication_codec.decode_response(response)
            if parameters["status"] != 0:
                raise RuntimeError("Authentication failed")
            conn.endpoint = parameters["address"]

        connection = self._client.connection_manager.get_or_connect(address, authenticate_manager)
        self.logger.info("Authenticated with %s", address)
        self.owner_connection_address = connection.endpoint
        self.init_membership_listener(connection)

    def init_membership_listener(self, connection):
        request = client_add_membership_listener_codec.encode_request(False)

        def handler(m):
            client_add_membership_listener_codec.handle(m, self.handle_member, self.handle_member_list)

        response = self._client.invoker.invoke_on_connection(request, connection, handler).result()
        registration_id = client_add_membership_listener_codec.decode_response(response)["response"]
        self.logger.debug("Registered membership listener with ID " + registration_id)

    def handle_member(self, member_event):
        self.logger.debug("got member event")
        self.logger.debug(member_event)

    def handle_member_list(self, member_list):
        self.logger.debug("got member list")
        self._client.partition_service.refresh()


class PartitionService(object):

    logger = logging.getLogger("PartitionService")

    def __init__(self, client):
        self.partitions = {}
        self._client = client
        self.t = None

    def start(self):
        def partition_updater():
            self._do_refresh()
            self.t = self._schedule_refresh(10)

        self.t = threading.Timer(10, partition_updater)
        self.t.setDaemon(True)
        self.t.start() # TODO: find a better scheduling option

    def refresh(self):
        self._schedule_refresh(0)

    def _schedule_refresh(self, delay):
        t = threading.Timer(delay, self._do_refresh)
        t.setDaemon(True)
        t.start()
        return t

    def _do_refresh(self):
        self.logger.debug("Start updating partitions")
        address = self._client.cluster.owner_connection_address
        connection = self._client.connection_manager.get_connection(address)
        if connection is None:
            self.logger.debug("Could not update partition thread as owner connection is not established yet.")
            return
        request = client_get_partitions_codec.encode_request()
        response = self._client.invoker.invoke_on_connection(request, connection).result()
        partitions = client_get_partitions_codec.decode_response(response)["partitions"]
        # TODO: needs sync
        self.partitions = {}
        for addr, partition_list in partitions.iteritems():
            for partition in partition_list:
                self.partitions[partition] = addr
        self.logger.debug("Finished updating partitions")
        # TODO: exception handling
