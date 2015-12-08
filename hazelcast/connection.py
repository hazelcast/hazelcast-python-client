import asyncore
import logging
from Queue import Queue
import socket
import threading

from hazelcast.protocol.client_message import ClientMessage, BEGIN_END_FLAG, LISTENER_FLAG
from hazelcast.protocol.codec import client_authentication_codec

BUFFER_SIZE = 4096
INVOCATION_TIMEOUT = 120
PROTOCOL_VERSION = 1


class InvocationService(object):
    logger = logging.getLogger("InvocationService")

    def __init__(self, client):
        self._pending = {}
        self._listeners = {}
        self._next_correlation_id = 1
        self._client = client
        self._event_queue = Queue()
        self._start_event_thread()

    def _start_event_thread(self):
        def event_loop():
            while True:
                event = self._event_queue.get()
                event[0](event[1])

        self._event_thread = threading.Thread(target=event_loop)
        self._event_thread.setDaemon(True)
        self._event_thread.start()

    def invoke_on_connection(self, message, connection, event_handler=None):
        return self._invoke(Invocation(message, connection=connection), event_handler)

    def invoke_on_partition(self, message, partition_id, event_handler=None):
        return self._invoke(Invocation(message, partition_id=partition_id), event_handler)

    def invoke_on_random_target(self, message, event_handler=None):
        return self._invoke(Invocation(message), event_handler)

    def invoke_on_target(self, message, address, event_handler=None):
        return self._invoke(Invocation(message, address=address), event_handler)

    def _invoke(self, invocation, event_handler):
        if event_handler is not None:
            invocation.handler = event_handler
        if invocation.has_connection():
            self._send(invocation, invocation.connection)
        elif invocation.has_partition_id():
            # TODO: set partition id on message
            addr = self._client.partition_service.get_partition_owner(invocation.partition_id)
            self._send_to_address(invocation, addr)
        elif invocation.has_address():
            self._send_to_address(invocation, invocation.address)
        else:  # send to random address
            addr = self._client.load_balancer.next_address()
            self._send_to_address(invocation, addr)

        return invocation

    def _send_to_address(self, invocation, address):
        conn = self._client.connection_manager.get_or_connect(address)
        self._send(invocation, conn)

    def _send(self, invocation, connection):
        correlation_id = self._next_correlation_id  # TODO: atomic integer equivalent
        self._next_correlation_id += 1
        message = invocation.message
        message.set_correlation_id(correlation_id)
        self._pending[correlation_id] = invocation

        if invocation.has_handler():
            self._listeners[correlation_id] = invocation

        self.logger.debug("Sending message with correlation id %s and type %s", correlation_id,
                          message.get_message_type())
        connection.send_message(message)

    def handle_client_message(self, message):
        correlation_id = message.get_correlation_id()
        self.logger.debug("Received message with correlation id %s and type %s", correlation_id,
                          message.get_message_type())
        if message.has_flags(LISTENER_FLAG):
            self.logger.debug("Got event message with type %d", message.get_message_type())
            if correlation_id not in self._listeners:
                self.logger.warn("Got event message with unknown correlation id: %d", correlation_id)
                return
            invocation = self._listeners[correlation_id]
            self._event_queue.put((invocation.handler, message))
            return

        if correlation_id not in self._pending:
            self.logger.warn("Got message with unknown correlation id: %d", correlation_id)
            return

        invocation = self._pending[correlation_id]
        invocation.response = message
        self._pending.pop(correlation_id)
        invocation.event.set()


class ConnectionManager(object):
    logger = logging.getLogger("ConnectionManager")

    def __init__(self, client, message_handler):
        self._io_thread = None
        self._client = client
        self.connections = {}
        self._message_handler = message_handler

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
        authenticator = authenticator or self._cluster_authenticator
        connection = Connection(address, self._message_handler)
        self._ensure_io()
        authenticator(connection)
        self.logger.info("Authenticated with %s", address)
        self.connections[connection.endpoint] = connection
        return connection

    def _ensure_io(self):
        if self._io_thread is None:
            self._io_thread = threading.Thread(target=self.io_loop)
            self._io_thread.daemon = True
            self._io_thread.start()

    def io_loop(self):
        asyncore.loop(count=None)
        self.logger.warn("IO Thread has exited.")


class Connection(asyncore.dispatcher):
    def __init__(self, address, message_handler):
        asyncore.dispatcher.__init__(self)
        self.logger = logging.getLogger("Connection{%s:%d}" % address)
        self._address = address
        self._correlation_id = 0
        self._message_handler = message_handler
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.endpoint = None
        self.connect(address)
        self._write_queue = Queue()
        self._write_queue.put("CB2")

    def handle_connect(self):
        self.logger.debug("Connected to %s", self._address)

    def handle_read(self):
        data = self.recv(BUFFER_SIZE)
        self.logger.debug("Read %d bytes", len(data))
        if len(data) == BUFFER_SIZE:  # TODO: more to read
            raise NotImplementedError()

        # split frames
        while len(data) > 0:
            message = ClientMessage(data)
            self._message_handler(message)
            frame_length = message.get_frame_length()
            data = data[frame_length:]

    def handle_write(self):
        self._initiate_send()

    def handle_close(self):
        self.logger.debug("handle_close")
        self.close()

    def writable(self):
        return not self._write_queue.empty()

    def _initiate_send(self):
        item = self._write_queue.get_nowait()
        sent = self.send(item)
        self.logger.debug("Written " + str(sent) + " bytes")
        # TODO: check if everything was sent

    def send_message(self, message):
        message.add_flag(BEGIN_END_FLAG)
        self._write_queue.put(message.buffer)
        self._initiate_send()


class Invocation(object):
    def __init__(self, message, partition_id=-1, address=None, connection=None):
        self.message = message
        self.event = threading.Event()
        self.response = None
        self.handler = None
        self.partition_id = partition_id
        self.address = address
        self.connection = connection

    def has_connection(self):
        return self.connection is not None

    def has_handler(self):
        return self.handler is not None

    def has_partition_id(self):
        return self.partition_id >= 0

    def has_address(self):
        return self.address is not None

    def result(self, timeout=INVOCATION_TIMEOUT):
        if self.event.wait(timeout):
            return self.response
        raise RuntimeError("Request timed out after %s seconds" % INVOCATION_TIMEOUT)
