from hazelcast.codec import client_authentication_codec, client_add_membership_listener_codec

class ClusterManager(object):
    def __init__(self, config, connection_manager):
        self._config = config
        self._connection_manager = connection_manager

        self.connect_to_cluster()

    def connect_to_cluster(self):
        address = self._config.get_addresses()[0]  # todo: multiple addresses
        print("Connecting to address " + address)

        def authenticate_manager(conn):
            request = client_authentication_codec.encode_request(
                self._config.username, self._config.password, None, None, True, "PHY", 1)
            response = conn.send_and_receive(request)
            parameters = client_authentication_codec.decode_response(response)
            print(parameters)

        connection = self._connection_manager.get_or_connect(address, authenticate_manager)
        self.init_membership_listener(connection)

    def init_membership_listener(self, connection):
        request = client_add_membership_listener_codec.encode_request(False)
        response = connection.send_and_receive(request)
        registration_id = client_add_membership_listener_codec.decode_response(response)["response"]
        print("Registered membership listener with ID " + registration_id)

