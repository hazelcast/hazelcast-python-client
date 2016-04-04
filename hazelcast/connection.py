from __future__ import with_statement
import logging
import struct
import threading
import time
from hazelcast.config import PROPERTY_HEARTBEAT_INTERVAL, PROPERTY_HEARTBEAT_TIMEOUT
from hazelcast.core import CLIENT_TYPE
from hazelcast.exception import AuthenticationError
from hazelcast.future import ImmediateFuture
from hazelcast.protocol.client_message import BEGIN_END_FLAG, ClientMessage, ClientMessageBuilder
from hazelcast.protocol.codec import client_authentication_codec, client_ping_codec
from hazelcast.serialization import INT_SIZE_IN_BYTES, FMT_LE_INT
from hazelcast.util import AtomicInteger

BUFFER_SIZE = 8192
PROTOCOL_VERSION = 1

DEFAULT_HEARTBEAT_INTERVAL = 5000
DEFAULT_HEARTBEAT_TIMEOUT = 60000


class ConnectionManager(object):
    logger = logging.getLogger("ConnectionManager")

    def __init__(self, client, new_connection_func):
        self._new_connection_mutex = threading.RLock()
        self._io_thread = None
        self._client = client
        self.connections = {}
        self._pending_connections = {}
        self._socket_map = {}
        self._new_connection_func = new_connection_func
        self._connection_listeners = []

    def add_listener(self, on_connection_opened=None, on_connection_closed=None):
        self._connection_listeners.append((on_connection_opened, on_connection_closed))

    def get_connection(self, address):
        try:
            return self.connections[address]
        except KeyError:
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
            parameters = client_authentication_codec.decode_response(f.result())
            if parameters["status"] != 0:
                raise AuthenticationError("Authentication failed.")
            connection.endpoint = parameters["address"]
            self.owner_uuid = parameters["owner_uuid"]
            self.uuid = parameters["uuid"]
            return connection

        return self._client.invoker.invoke_on_connection(request, connection).continue_with(callback)

    def get_or_connect(self, address, authenticator=None):
        if address in self.connections:
            return ImmediateFuture(self.connections[address])
        else:
            with self._new_connection_mutex:
                if address in self._pending_connections:
                    return self._pending_connections[address]
                else:
                    authenticator = authenticator or self._cluster_authenticator
                    connection = self._new_connection_func(address,
                                                           connection_closed_callback=self._connection_closed,
                                                           message_callback=self._client.invoker._handle_client_message)

                    def on_auth(f):
                        if f.is_success():
                            self.logger.info("Authenticated with %s", f.result())
                            with self._new_connection_mutex:
                                self.connections[connection.endpoint] = f.result()
                                self._pending_connections.pop(address)
                            for on_connection_opened, _ in self._connection_listeners:
                                if on_connection_opened:
                                    on_connection_opened(f.resul())
                            return f.result()
                        else:
                            self.logger.debug("Error opening %s", connection)
                            with self._new_connection_mutex:
                                try:
                                    self._pending_connections.pop(address)
                                except KeyError:
                                    pass
                            raise f.exception(), None, f.traceback()

                    future = authenticator(connection).continue_with(on_auth)
                    if not future.done():
                        self._pending_connections[address] = future
                    return future

    def _connection_closed(self, connection, cause):
        # if connection was authenticated, fire event
        if connection.endpoint:
            self.connections.pop(connection.endpoint)
            for _, on_connection_closed in self._connection_listeners:
                if on_connection_closed:
                    on_connection_closed(connection, cause)
        else:
            # clean-up unauthenticated connection
            self._client.invoker.cleanup_connection(connection, cause)

    def close_connection(self, address, cause):
        try:
            connection = self.connections[address]
            connection.close(cause)
        except KeyError:
            logging.warn("No connection with %s was found to close.", address)
            return False


class Heartbeat(object):
    logger = logging.getLogger("ConnectionManager")
    _heartbeat_timer = None

    def __init__(self, client):
        self._client = client
        self._listeners = []

        self._heartbeat_timeout = client.config.get_property_or_default(PROPERTY_HEARTBEAT_TIMEOUT,
                                                                        DEFAULT_HEARTBEAT_TIMEOUT) / 1000
        self._heartbeat_interval = client.config.get_property_or_default(PROPERTY_HEARTBEAT_INTERVAL,
                                                                         DEFAULT_HEARTBEAT_INTERVAL) / 1000

    def start(self):
        def _heartbeat():
            self._heartbeat()
            self._heartbeat_timer = self._client.reactor.add_timer(self._heartbeat_interval, _heartbeat)

        self._heartbeat_timer = self._client.reactor.add_timer(self._heartbeat_interval, _heartbeat)

    def shutdown(self):
        if self._heartbeat():
            self._heartbeat_timer.cancel()

    def add_listener(self, on_heartbeat_restored=None, on_heartbeat_stopped=None):
        self._listeners.append((on_heartbeat_restored, on_heartbeat_stopped))

    def _heartbeat(self):
        now = time.time()
        for connection in self._client.connection_manager.connections.values():
            time_since_last_read = now - connection.last_read
            if time_since_last_read > self._heartbeat_timeout:
                if connection.heartbeating:
                    self.logger.warn(
                        "Heartbeat: Did not hear back after %ss from %s" % (time_since_last_read, connection))
                    self._on_heartbeat_stopped(connection)

            if time_since_last_read > self._heartbeat_interval:
                request = client_ping_codec.encode_request()
                self._client.invoker.invoke_on_connection(request, connection, ignore_heartbeat=True)
            else:
                if not connection.heartbeating:
                    self._on_heartbeat_restored(connection)

    def _on_heartbeat_restored(self, connection):
        self.logger.info("Heartbeat: Heartbeat restored for connection %s" % connection)
        connection.heartbeating = True
        for callback, _ in self._listeners:
            if callback:
                callback(connection)

    def _on_heartbeat_stopped(self, connection):
        connection.heartbeating = False
        for _, callback in self._listeners:
            if callback:
                callback(connection)


class Connection(object):
    _closed = False
    endpoint = None
    heartbeating = True
    is_owner = False
    counter = AtomicInteger()

    def __init__(self, address, connection_closed_callback, message_callback):
        self._address = (address.host, address.port)
        self.id = self.counter.get_and_increment()
        self.logger = logging.getLogger("Connection[%s](%s:%d)" % (self.id, address.host, address.port))
        self._connection_closed_callback = connection_closed_callback
        self._builder = ClientMessageBuilder(message_callback)
        self._read_buffer = ""
        self.last_read = 0

    def live(self):
        return not self._closed

    def send_message(self, message):
        if not self.live():
            raise IOError("Connection is not live.")

        message.add_flag(BEGIN_END_FLAG)
        self.write(message.buffer)

    def receive_message(self):
        self.last_read = time.time()
        # split frames
        while len(self._read_buffer) >= INT_SIZE_IN_BYTES:
            frame_length = struct.unpack_from(FMT_LE_INT, self._read_buffer, 0)[0]
            if frame_length > len(self._read_buffer):
                return
            message = ClientMessage(buffer(self._read_buffer, 0, frame_length))
            self._read_buffer = self._read_buffer[frame_length:]
            self._builder.on_message(message)

    def write(self, data):
        # must be implemented by subclass
        raise NotImplementedError

    def close(self, cause):
        raise NotImplementedError

    def __repr__(self):
        return "Connection(address=%s, id=%s)" % (self._address, self.id)
