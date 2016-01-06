from __future__ import with_statement
import logging
import threading
import struct
from hazelcast.core import CLIENT_TYPE
from hazelcast.exception import AuthenticationError
from hazelcast.future import ImmediateFuture
from hazelcast.invocation import Future
from hazelcast.protocol.client_message import BEGIN_END_FLAG, ClientMessage
from hazelcast.protocol.codec import client_authentication_codec
from hazelcast.serialization import INT_SIZE_IN_BYTES, FMT_LE_INT

BUFFER_SIZE = 8192
PROTOCOL_VERSION = 1


class ConnectionManager(object):
    logger = logging.getLogger("ConnectionManager")

    def __init__(self, client, new_connection_type):
        self._new_connection_mutex = threading.Lock()
        self._io_thread = None
        self._client = client
        self.connections = {}
        self._pending_connections = {}
        self._socket_map = {}
        self._new_connection = new_connection_type
        self._connnection_listeners = []

    def add_listener(self, on_connection_opened=None, on_connection_closed=None):
        self._connnection_listeners.append((on_connection_opened, on_connection_closed))

    def get_connection(self, address):
        if address in self.connections:
            return self.connections[address]
        return None

    def _cluster_authenticator(self, connection):
        uuid = self._client.cluster.uuid
        owner_uuid = self._client.cluster.owner_uuid

        request = client_authentication_codec.encode_request(
            username=self._client.config.group_config.name,
            password=self._client.config.group_config.password,
            uuid=uuid,
            owner_uuid=owner_uuid,
            is_owner_connection=False,
            client_type=CLIENT_TYPE,
            serialization_version=1)

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

    def get_or_connect(self, address, authenticator=None):
        if address in self.connections:
            return ImmediateFuture(self.connections[address])
        else:
            with self._new_connection_mutex:
                if address in self._pending_connections:
                    return self._pending_connections[address]
                else:
                    def on_auth(f):
                        if f.is_success():
                            self.logger.info("Authenticated with %s", address)
                            with self._new_connection_mutex:
                                self.connections[connection.endpoint] = connection
                                self._pending_connections.pop(address)
                            for on_connection_opened, _ in self._connnection_listeners:
                                if on_connection_opened:
                                    on_connection_opened(connection)
                            return connection
                        else:
                            self._pending_connections.pop(address)
                            raise f.exception()
                authenticator = authenticator or self._cluster_authenticator
                connection = self._new_connection(address, connection_closed_cb=self._connection_closed)
                future = authenticator(connection).continue_with(on_auth)
                self._pending_connections[address] = future
                return future

    def _connection_closed(self, connection):
        if connection.endpoint:
            self.connections.pop(connection.endpoint)

        for _, on_connection_closed in self._connnection_listeners:
            if on_connection_closed:
                on_connection_closed(connection)

    def heartbeat(self):
        # TODO
        pass


class Connection(object):
    _closed = False
    callback = None
    endpoint = None

    def __init__(self, address, connection_closed_cb):
        self._address = (address.host, address.port)
        self.logger = logging.getLogger("Connection{%s:%d}" % self._address)
        self._connection_closed_cb = connection_closed_cb
        self._read_buffer = ""

    def send_message(self, message):
        if self._closed:
            raise IOError("Connection is not live.")

        message.add_flag(BEGIN_END_FLAG)
        self.write(message.buffer)

    def receive_message(self):
        # split frames
        while len(self._read_buffer) >= INT_SIZE_IN_BYTES:
            frame_length = struct.unpack_from(FMT_LE_INT, self._read_buffer, 0)[0]
            if frame_length > len(self._read_buffer):
                return
            message = ClientMessage(buffer(self._read_buffer, 0, frame_length))
            self._read_buffer = self._read_buffer[frame_length:]
            self.callback(message)

    def write(self, data):
        # must be implemented by subclass
        pass
