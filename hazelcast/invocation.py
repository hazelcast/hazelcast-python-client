from Queue import Queue, Empty
import logging
import threading
import time

from hazelcast.protocol.client_message import LISTENER_FLAG
from hazelcast.util import AtomicInteger

EVENT_LOOP_COUNT = 100
INVOCATION_TIMEOUT = 120


class Invocation(object):

    _response = None
    _exception = None
    timer = None
    handler = None
    sent_connection = None

    def __init__(self, request, partition_id=-1, address=None, connection=None, timeout=INVOCATION_TIMEOUT,
                 callback=None, error_callback=None):
        self._event = threading.Event()
        self.timeout = timeout + time.time()
        self.address = address
        self.callback = callback
        self.error_callback = error_callback
        self.connection = connection
        self.partition_id = partition_id
        self.request = request

    def has_connection(self):
        return self.connection is not None

    def has_handler(self):
        return self.handler is not None

    def has_partition_id(self):
        return self.partition_id >= 0

    def has_address(self):
        return self.address is not None

    def set_response(self, response):
        self._response = response
        self._event.set()
        if self.timer:
            self.timer.cancel()
        if self.callback:
            self.callback(response)

    def set_exception(self, exception):
        self._exception = exception
        self._event.set()
        if self.timer:
            self.timer.cancel()
        if self.error_callback:
            self.error_callback(exception)

    def result(self):
        self._event.wait()
        if self._response is not None:
            return self._response
        raise self._exception

    def on_timeout(self):
        self.set_exception(RuntimeError("Requested timed out."))

    @property
    def completed(self):
        return self._response is not None or self._exception is not None


class InvocationService(object):
    logger = logging.getLogger("InvocationService")

    def __init__(self, client):
        self._pending = {}
        self._listeners = {}
        self._next_correlation_id = AtomicInteger(1)
        self._client = client
        self._event_queue = Queue()

    def handle_event(self, invocation, message):
        try:
            invocation.handler(message)
        except:
            self.logger.warn("Error handling event %s", message, exc_info=True)

    def invoke_on_connection(self, message, connection, event_handler=None, callback=None):
        return self._invoke(Invocation(message, connection=connection, callback=callback), event_handler)

    def invoke_on_partition(self, message, partition_id, event_handler=None, callback=None):
        return self._invoke(Invocation(message, partition_id=partition_id, callback=callback), event_handler)

    def invoke_on_random_target(self, message, event_handler=None, callback=None):
        return self._invoke(Invocation(message, callback=callback), event_handler)

    def invoke_on_target(self, message, address, event_handler=None, callback=None):
        return self._invoke(Invocation(message, address=address, callback=callback), event_handler)

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
        self._pending[correlation_id] = invocation
        if not invocation.timer:
            invocation.timer = self._client.reactor.add_timer(invocation.timeout, invocation.on_timeout)

        if invocation.has_handler():
            self._listeners[correlation_id] = invocation

        self.logger.debug("Sending message with correlation id %s and type %s", correlation_id,
        message.get_message_type())

        if not connection.callback:
            connection.callback = self.handle_client_message
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
            self.handle_event(invocation, message)
            return
        if correlation_id not in self._pending:
            self.logger.warn("Got message with unknown correlation id: %d", correlation_id)
            return
        invocation = self._pending.pop(correlation_id)
        invocation.set_response(message)


def handle_exception(self, invocation, error):
    self.logger.debug("Got exception for request with correlation id %d: %s", invocation.get_correlation_id(),
                      error)
    if isinstance(error, IOError):
        pass
        # TODO: retry

    invocation.set_exception(error)


def connection_closed(self, connection):
    for correlation_id, invocation in dict(self._pending).iteritems():
        if invocation.connection == connection:
            invocation.set_exception(IOError("Connection to server was closed."))
            # fail invocation

    for correlation_id, invocation in dict(self._listeners).iteritems():
        if invocation.connection == connection:
            self._listeners.pop(correlation_id)
            # TODO: re-register listener
