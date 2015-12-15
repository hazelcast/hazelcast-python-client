from __future__ import with_statement

import logging
import threading
import struct

from hazelcast.protocol.client_message import BEGIN_END_FLAG, ClientMessage
from hazelcast.protocol.codec import client_authentication_codec
from hazelcast.serialization import INT_SIZE_IN_BYTES, FMT_LE_INT

BUFFER_SIZE = 8192
PROTOCOL_VERSION = 1


class ConnectionManager(object):
    logger = logging.getLogger("ConnectionManager")

    def __init__(self, client, connection_type):
        self._new_connection_mutex = threading.Lock()
        self._io_thread = None
        self._client = client
        self.connections = {}
        self._socket_map = {}
        self._connection_type = connection_type

    def get_connection(self, address):
        if address in self.connections:
            return self.connections[address]
        return None

    def _cluster_authenticator(self, conn):
        uuid = self._client.cluster.uuid
        owner_uuid = self._client.cluster.owner_uuid

        request = client_authentication_codec.encode_request(
            username=self._client.config.group_config.name,
            password=self._client.config.group_config.password,
            uuid=uuid,
            owner_uuid=owner_uuid,
            is_owner_connection=False,
            client_type="PHY",
            serialization_version=1)

        response = self._client.invoker.invoke_on_connection(request, conn).result()
        parameters = client_authentication_codec.decode_response(response)
        if parameters["status"] != 0:
            raise RuntimeError("Authentication failed")
        conn.endpoint = parameters["address"]
        self.owner_uuid = parameters["owner_uuid"]
        self.uuid = parameters["uuid"]
        pass

    def get_or_connect(self, address, authenticator=None):
        if address in self.connections:
            return self.connections[address]
        else:
            with self._new_connection_mutex:
                if address in self.connections:
                    return self.connections[address]
                authenticator = authenticator or self._cluster_authenticator
                connection = self._connection_type(address, connection_closed_cb=self.connection_closed)
                authenticator(connection)
                self.logger.info("Authenticated with %s", address)
                self.connections[connection.endpoint] = connection
                return connection

    def connection_closed(self, connection):
        if connection.endpoint:
            self.connections.pop(connection.endpoint)
        self._client.invoker.connection_closed(connection)


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
            raise RuntimeError("Connection is not live.")

        message.add_flag(BEGIN_END_FLAG)
        self.write(message.buffer)

    def receive_message(self):
        # split frames
        while len(self._read_buffer) >= INT_SIZE_IN_BYTES:
            frame_length = struct.unpack_from(FMT_LE_INT, self._read_buffer, 0)[0]
            if frame_length > len(self._read_buffer):
                self.logger.debug("Message is not yet complete")
                return
            message = ClientMessage(buffer(self._read_buffer, 0, frame_length))
            self._read_buffer = self._read_buffer[frame_length:]
            self.callback(message)

    def write(self, data):
        # must be implemented by subclass
        pass
