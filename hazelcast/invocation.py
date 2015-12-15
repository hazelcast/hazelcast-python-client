from Queue import Queue, Empty
import logging
import threading
import time

from hazelcast.protocol.client_message import LISTENER_FLAG
from hazelcast.util import AtomicInteger

EVENT_LOOP_COUNT = 100
INVOCATION_TIMEOUT = 120

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
        self.sent_connection = None

    def has_connection(self):
        return self.connection is not None

    def has_handler(self):
        return self.handler is not None

    def has_partition_id(self):
        return self.partition_id >= 0

    def has_address(self):
        return self.address is not None

    def check_timer(self, now):
        if self.completed:
            return True

        if now >= self._timeout:
            self.set_exception(RuntimeError("Request timed out."))
            return True

        return False

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

class InvocationService(object):
    logger = logging.getLogger("InvocationService")

    def __init__(self, client):
        self._is_live = False
        self._pending = {}
        self._listeners = {}
        self._next_correlation_id = AtomicInteger(1)
        self._client = client
        self._event_queue = Queue()

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
        invocation = self._pending.pop(correlation_id)
        invocation.set_response(message)

    def handle_exception(self, invocation, error):
        self.logger.debug("Got exception for request with correlation id %d: %s", invocation.get_correlation_id(), error)
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

