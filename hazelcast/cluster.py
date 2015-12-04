import threading
from hazelcast.codec import client_authentication_codec, \
    client_add_membership_listener_codec, \
    client_get_partitions_codec


class ClusterManager(object):
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
        address = self._parse_addr(self._config.get_addresses()[0])
        print("Connecting to address ", address)

        def authenticate_manager(conn):
            request = client_authentication_codec.encode_request(
                self._config.username, self._config.password, None, None, True, "PHY", 1)
            response = conn.send_and_receive(request)
            parameters = client_authentication_codec.decode_response(response)
            if parameters["status"] != 0:
                raise RuntimeError("Authentication failed")
            conn.endpoint = parameters["address"]

        connection = self._client.connection_manager.get_or_connect(address, authenticate_manager)
        print("Authenticated with ", address)
        self.owner_connection_address = connection.endpoint
        self.init_membership_listener(connection)

    def init_membership_listener(self, connection):
        request = client_add_membership_listener_codec.encode_request(False)

        def handler(m):
            client_add_membership_listener_codec.handle(m, self.handle_member, self.handle_member_list)

        response = connection.start_listening(request, handler)
        registration_id = client_add_membership_listener_codec.decode_response(response)["response"]
        print("Registered membership listener with ID " + registration_id)

    def handle_member(self, member_event):
        print("got member event")
        print(member_event)

    def handle_member_list(self, member_list):
        print("got member list")
        self._client.partition_service.refresh()


class PartitionService(object):
    def __init__(self, cluster, connection_manager):
        self.partitions = {}
        self._cluster = cluster
        self._connection_manager = connection_manager
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
        print("Updating partitions")
        address = self._cluster.owner_connection_address
        connection = self._connection_manager.get_connection(address)
        if connection is None:
            print("Could not update partition thread as owner connection is not established yet.")
            return
        request = client_get_partitions_codec.encode_request()
        response = connection.send_and_receive(request)
        partitions = client_get_partitions_codec.decode_response(response)["partitions"]
        # TODO: needs sync
        self.partitions = {}
        for addr, partition_list in partitions.iteritems():
            for partition in partition_list:
                self.partitions[partition] = addr
        print("Updated partitions")
        # TODO: exception handling
