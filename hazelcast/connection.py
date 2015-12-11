from __future__ import with_statement

import asyncore
import logging
from Queue import Queue, Empty
import socket
import threading
import struct
import time

from hazelcast.protocol.client_message import ClientMessage, BEGIN_END_FLAG, LISTENER_FLAG
from hazelcast.protocol.codec import client_authentication_codec
from hazelcast.serialization import FMT_LE_INT, INT_SIZE_IN_BYTES

EVENT_LOOP_COUNT = 100

BUFFER_SIZE = 8192
INVOCATION_TIMEOUT = 120
PROTOCOL_VERSION = 1


class AtomicInteger(object):
    def __init__(self, initial=0):
        self.lock = threading.Lock()
        self.initial = initial

    def increment_and_get(self):
        with self.lock:
            self.initial += 1
            return self.initial


class InvocationService(object):
    logger = logging.getLogger("InvocationService")

    def __init__(self, client):
        self._is_live = False
        self._pending = {}
        self._listeners = {}
        self._next_correlation_id = AtomicInteger(1)
        self._client = client
        self._event_queue = Queue()
        self._pending_lock = threading.Lock()

    def start(self):
        self._is_live = True
        self._start_event_thread()

    def shutdown(self):
        self._is_live = False
        self._event_thread.join()

    def _start_event_thread(self):
        def handle_event(event):
            try:
                event[0](event[1])
            except:
                self.logger.warn("Error handling event %s", event, exc_info=True)

        def service_timeouts():
            now = time.time()
            for correlation_id, invocation in dict(self._pending).iteritems():
                if invocation.check_timer(now):
                    try:
                        self._pending.pop(correlation_id)
                    except KeyError:
                        pass

        def event_loop():
            self.logger.debug("Starting event thread")
            while True and self._is_live:
                for _ in xrange(0, EVENT_LOOP_COUNT):
                    try:
                        event = self._event_queue.get(timeout=0.01)
                        handle_event(event)
                    except Empty:
                        break
                service_timeouts()

        self.logger.debug("Event thread exited.")
        self._event_thread = threading.Thread(target=event_loop, name="hazelcast-event-handler-loop")
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
        correlation_id = self._next_correlation_id.increment_and_get()
        message = invocation.request
        message.set_correlation_id(correlation_id)
        message.set_partition_id(invocation.partition_id)
        with self._pending_lock:
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
        with self._pending_lock:
            if correlation_id not in self._pending:
                self.logger.warn("Got message with unknown correlation id: %d", correlation_id)
                return
            invocation = self._pending.pop(correlation_id)
        invocation.set_response(message)

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


class Invocation(object):
    def __init__(self, request, partition_id=-1, address=None, connection=None, timeout=INVOCATION_TIMEOUT):
        self._event = threading.Event()
        self._response = None
        self._exception = None
        self._timeout = timeout + time.time()
        self.request = request
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

    def check_timer(self, now):
        if self.completed or now < self._timeout:
            return False
        self.set_exception(RuntimeError("Request timed out."))
        return True

    def set_response(self, response):
        self._response = response
        self._event.set()

    def set_exception(self, exception):
        self._exception = exception
        self._event.set()

    def result(self):
        self._event.wait()
        if self._response is not None:
            return self._response
        raise self._exception

    @property
    def completed(self):
        return self._response is not None or self._exception is not None