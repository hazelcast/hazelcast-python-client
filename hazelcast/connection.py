from __future__ import with_statement

import logging
import struct
import sys
import threading
import time

from hazelcast.core import CLIENT_TYPE
from hazelcast.exception import AuthenticationError
from hazelcast.future import ImmediateFuture, ImmediateExceptionFuture
from hazelcast.protocol.client_message import BEGIN_END_FLAG, ClientMessage, ClientMessageBuilder
from hazelcast.protocol.codec import client_authentication_codec, client_ping_codec
from hazelcast.serialization import INT_SIZE_IN_BYTES, FMT_LE_INT
from hazelcast.util import AtomicInteger, parse_addresses
from hazelcast import six

BUFFER_SIZE = 8192
PROTOCOL_VERSION = 1


class ConnectionManager(object):
    """
    ConnectionManager is responsible for managing :mod:`Connection` objects.
    """
    logger = logging.getLogger("ConnectionManager")

    def __init__(self, client, new_connection_func, address_translator):
        self._new_connection_mutex = threading.RLock()
        self._io_thread = None
        self._client = client
        self.connections = {}
        self._pending_connections = {}
        self._socket_map = {}
        self._new_connection_func = new_connection_func
        self._connection_listeners = []
        self._address_translator = address_translator

    def add_listener(self, on_connection_opened=None, on_connection_closed=None):
        """
        Registers a ConnectionListener. If the same listener is registered multiple times, it will be notified multiple
        times.

        :param on_connection_opened: (Function), function to be called when a connection is opened.
        :param on_connection_closed: (Function), function to be called when a connection is removed.
        """
        self._connection_listeners.append((on_connection_opened, on_connection_closed))

    def get_connection(self, address):
        """
        Gets the existing connection for a given address or connects. This call is silent.

        :param address: (:class:`~hazelcast.core.Address`), the address to connect to.
        :return: (:class:`~hazelcast.connection.Connection`), the found connection, or None if no connection exists.
        """
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
        """
        Gets the existing connection for a given address. If it does not exist, the system will try to connect
        asynchronously. In this case, it returns a Future. When the connection is established at some point in time, it
        can be retrieved by using the get_connection(:class:`~hazelcast.core.Address`) or from Future.

        :param address: (:class:`~hazelcast.core.Address`), the address to connect to.
        :param authenticator: (Function), function to be used for authentication (optional).
        :return: (:class:`~hazelcast.connection.Connection`), the existing connection or it returns a Future which includes asynchronously.
        """
        if address in self.connections:
            return ImmediateFuture(self.connections[address])
        else:
            with self._new_connection_mutex:
                if address in self._pending_connections:
                    return self._pending_connections[address]
                else:
                    authenticator = authenticator or self._cluster_authenticator
                    try:
                        translated_address = self._address_translator.translate(address)
                        if translated_address is None:
                            raise ValueError("Address translator could not translate address: {}".format(address))
                        connection = self._new_connection_func(translated_address,
                                                               self._client.config.network_config.connection_timeout,
                                                               self._client.config.network_config.socket_options,
                                                               connection_closed_callback=self._connection_closed,
                                                               message_callback=self._client.invoker._handle_client_message,
                                                               network_config=self._client.config.network_config)
                    except IOError:
                        return ImmediateExceptionFuture(sys.exc_info()[1], sys.exc_info()[2])

                    future = authenticator(connection).continue_with(self.on_auth, connection, address)
                    if not future.done():
                        self._pending_connections[address] = future
                    return future

    def on_auth(self, f, connection, address):
        """
        Checks for authentication of a connection.

        :param f: (:class:`~hazelcast.future.Future`), future that contains the result of authentication.
        :param connection: (:class:`~hazelcast.connection.Connection`), newly established connection.
        :param address: (:class:`~hazelcast.core.Address`), the adress of new connection.
        :return: Result of authentication.
        """
        if f.is_success():
            self.logger.info("Authenticated with %s", f.result())
            with self._new_connection_mutex:
                self.connections[connection.endpoint] = f.result()
                try:
                    self._pending_connections.pop(address)
                except KeyError:
                    pass
            for on_connection_opened, _ in self._connection_listeners:
                if on_connection_opened:
                    on_connection_opened(f.result())
            return f.result()
        else:
            self.logger.debug("Error opening %s", connection)
            with self._new_connection_mutex:
                try:
                    self._pending_connections.pop(address)
                except KeyError:
                    pass
            six.reraise(f.exception().__class__, f.exception(), f.traceback())

    def _connection_closed(self, connection, cause):
        # if connection was authenticated, fire event
        if connection.endpoint:
            try:
                self.connections.pop(connection.endpoint)
            except KeyError:
                pass
            for _, on_connection_closed in self._connection_listeners:
                if on_connection_closed:
                    on_connection_closed(connection, cause)
        else:
            # clean-up unauthenticated connection
            self._client.invoker.cleanup_connection(connection, cause)

    def close_connection(self, address, cause):
        """
        Closes the connection with given address.

        :param address: (:class:`~hazelcast.core.Address`), address of the connection to be closed.
        :param cause: (Exception), the cause for closing the connection.
        :return: (bool), ``true`` if the connection is closed, ``false`` otherwise.
        """
        try:
            connection = self.connections[address]
            connection.close(cause)
        except KeyError:
            logging.warning("No connection with %s was found to close.", address)
            return False


class Heartbeat(object):
    """
    HeartBeat Service.
    """
    logger = logging.getLogger("ConnectionManager")
    _heartbeat_timer = None

    def __init__(self, client):
        self._client = client
        self._listeners = []

        self._heartbeat_timeout = client.properties.get_seconds_positive_or_default(client.properties.HEARTBEAT_TIMEOUT)
        self._heartbeat_interval = client.properties.get_seconds_positive_or_default(client.properties.HEARTBEAT_INTERVAL)

    def start(self):
        """
        Starts sending periodic HeartBeat operations.
        """
        def _heartbeat():
            if not self._client.lifecycle.is_live:
                return
            self._heartbeat()
            self._heartbeat_timer = self._client.reactor.add_timer(self._heartbeat_interval, _heartbeat)

        self._heartbeat_timer = self._client.reactor.add_timer(self._heartbeat_interval, _heartbeat)

    def shutdown(self):
        """
        Stops HeartBeat operations.
        """
        if self._heartbeat():
            self._heartbeat_timer.cancel()

    def add_listener(self, on_heartbeat_restored=None, on_heartbeat_stopped=None):
        """
        Registers a HeartBeat listener. Listener is invoked when a HeartBeat related event occurs.

        :param on_heartbeat_restored: (Function), function to be called when a HeartBeat is restored (optional).
        :param on_heartbeat_stopped:  (Function), function to be called when a HeartBeat is stopped (optional).
        """
        self._listeners.append((on_heartbeat_restored, on_heartbeat_stopped))

    def _heartbeat(self):
        now = time.time()
        for connection in list(self._client.connection_manager.connections.values()):
            time_since_last_read = now - connection.last_read
            time_since_last_write = now - connection.last_write
            if time_since_last_read > self._heartbeat_timeout:
                if connection.heartbeating:
                    self.logger.warning(
                        "Heartbeat: Did not hear back after %ss from %s" % (time_since_last_read, connection))
                    self._on_heartbeat_stopped(connection)
            else:
                if not connection.heartbeating:
                    self._on_heartbeat_restored(connection)

            if time_since_last_write > self._heartbeat_interval:
                request = client_ping_codec.encode_request()
                self._client.invoker.invoke_on_connection(request, connection, ignore_heartbeat=True)

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
    """
    Connection object which stores connection related information and operations.
    """
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
        self._read_buffer = b""
        self.last_read = time.time()
        self.last_write = 0

    def live(self):
        """
        Determines whether this connection is live or not.

        :return: (bool), ``true`` if the connection is live, ``false`` otherwise.
        """
        return not self._closed

    def send_message(self, message):
        """
        Sends a message to this connection.

        :param message: (Message), message to be sent to this connection.
        """
        if not self.live():
            raise IOError("Connection is not live.")

        message.add_flag(BEGIN_END_FLAG)
        self.write(message.buffer)

    def receive_message(self):
        """
        Receives a message from this connection.
        """
        self.last_read = time.time()
        # split frames
        while len(self._read_buffer) >= INT_SIZE_IN_BYTES:
            frame_length = struct.unpack_from(FMT_LE_INT, self._read_buffer, 0)[0]
            if frame_length > len(self._read_buffer):
                return
            message = ClientMessage(memoryview(self._read_buffer)[:frame_length])
            self._read_buffer = self._read_buffer[frame_length:]
            self._builder.on_message(message)

    def write(self, data):
        """
        Writes data to this connection when sending messages.

        :param data: (Data), data to be written to connection.
        """
        # must be implemented by subclass
        raise NotImplementedError

    def close(self, cause):
        """
        Closes the connection.

        :param cause: (Exception), the cause of closing the connection.
        """
        raise NotImplementedError

    def __repr__(self):
        return "Connection(address=%s, id=%s)" % (self._address, self.id)


class DefaultAddressProvider(object):
    """
    Provides initial addresses for client to find and connect to a node.
    Loads addresses from the Hazelcast configuration.
    """
    def __init__(self, network_config):
        self._network_config = network_config

    def load_addresses(self):
        """
        :return: (Sequence), The possible member addresses to connect to.
        """
        return parse_addresses(self._network_config.addresses)


class DefaultAddressTranslator(object):
    """
    DefaultAddressTranslator is a no-op. It always returns the given address.
    """
    def translate(self, address):
        """
        :param address: (:class:`~hazelcast.core.Address`), address to be translated.
        :return: (:class:`~hazelcast.core.Address`), translated address.
        """
        return address

    def refresh(self):
        """Refreshes the internal lookup table if necessary."""
        pass
