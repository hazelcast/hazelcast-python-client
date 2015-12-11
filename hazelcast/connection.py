from __future__ import with_statement

import asyncore
import logging
from Queue import Queue, Empty
import socket
import threading
import struct

from hazelcast.protocol.client_message import ClientMessage, BEGIN_END_FLAG
from hazelcast.protocol.codec import client_authentication_codec
from hazelcast.serialization import FMT_LE_INT, INT_SIZE_IN_BYTES

BUFFER_SIZE = 8192
PROTOCOL_VERSION = 1

class ConnectionManager(object):
    logger = logging.getLogger("ConnectionManager")

    def __init__(self, client, message_handler):
        self._new_connection_mutex = threading.Lock()
        self._io_thread = None
        self._client = client
        self.connections = {}
        self._socket_map = {}
        self._message_handler = message_handler
        self._is_live = False

    def start(self):
        self._is_live = True
        self._start_io_loop()

    def get_connection(self, address):
        if address in self.connections:
            return self.connections[address]
        return None

    def _cluster_authenticator(self, conn):
        uuid = self._client.cluster.uuid
        owner_uuid = self._client.cluster.owner_uuid

        request = client_authentication_codec.encode_request(
            username=self._client.config.username,
            password=self._client.config.password,
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
                connection = Connection(address, self._message_handler, socket_map=self._socket_map)
                authenticator(connection)
                self.logger.info("Authenticated with %s", address)
                self.connections[connection.endpoint] = connection
                return connection

    def _start_io_loop(self):
        self._io_thread = threading.Thread(target=self.io_loop, name="hazelcast-io-loop")
        self._io_thread.daemon = True
        self._io_thread.start()

    def io_loop(self):
        self.logger.debug("Starting IO Thread")
        while self._is_live:
            try:
                asyncore.loop(count=100, timeout=0.01, map=self._socket_map)
            except:
                self.logger.warn("IO Thread exited unexpectedly", exc_info=True)
        self.logger.debug("IO Thread exited.")

    def shutdown(self):
        asyncore.close_all(self._socket_map)
        self._is_live = False
        self._io_thread.join()

class Connection(asyncore.dispatcher):
    def __init__(self, address, message_handler, socket_map):
        asyncore.dispatcher.__init__(self, map=socket_map)
        self._address = (address.host, address.port)
        self.logger = logging.getLogger("Connection{%s:%d}" % self._address)
        self._message_handler = message_handler
        self._write_queue = Queue()
        self._write_queue.put("CB2")
        self._read_buffer = ""
        self.endpoint = None

        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect(self._address)

    def handle_connect(self):
        self.logger.debug("Connected to %s", self._address)

    def handle_read(self):
        self._read_buffer += self.recv(BUFFER_SIZE)
        self.logger.debug("Read %d bytes", len(self._read_buffer))
        # split frames
        while len(self._read_buffer) >= INT_SIZE_IN_BYTES:
            frame_length = struct.unpack_from(FMT_LE_INT, self._read_buffer, 0)[0]
            if frame_length > len(self._read_buffer):
                self.logger.debug("Message is not yet complete")
                return
            message = ClientMessage(buffer(self._read_buffer, 0, frame_length))
            self._read_buffer = self._read_buffer[frame_length:]
            self._message_handler(message)

    def handle_write(self):
        try:
            item = self._write_queue.get_nowait()
        except Empty:
            return
        sent = self.send(item)
        self.logger.debug("Written " + str(sent) + " bytes")
        if sent < len(item):
            self.logger.warn("%d bytes were not written", len(item) - sent)

    def handle_close(self):
        self.logger.debug("handle_close")
        self.close()

    def send_message(self, message):
        message.add_flag(BEGIN_END_FLAG)
        self._write_queue.put(message.buffer)
