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
            if parameters["status"] != 0:
                raise RuntimeError("Authentication failed")
        connection = self._connection_manager.get_or_connect(address, authenticate_manager)
        print("Authenticated with " + address)
        self.init_membership_listener(connection)

    def init_membership_listener(self, connection):
        request = client_add_membership_listener_codec.encode_request(False)
        import functools
        def handler(m):
            client_add_membership_listener_codec.handle(m, self.handle_member, self.handle_member_list)

        response = connection.start_listening(request, handler)
        registration_id = client_add_membership_listener_codec.decode_response(response)["response"]
        print("Registered membership listener with ID " + registration_id)

    def handle_member(self, member):
        print("handle_member")
        print(member)

    def handle_member_list(self, member_list):
        print("Got member list:")
        print(member_list)