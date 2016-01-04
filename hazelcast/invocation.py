import logging
import sys
import threading
import time
from Queue import Queue

from hazelcast.exception import create_exception
from hazelcast.protocol.client_message import LISTENER_FLAG
from hazelcast.protocol.custom_codec import EXCEPTION_MESSAGE_TYPE, ErrorCodec
from hazelcast.util import AtomicInteger

EVENT_LOOP_COUNT = 100
INVOCATION_TIMEOUT = 120


class Future(object):
    _result = None
    _exception = None
    logger = logging.getLogger("Future")

    def __init__(self):
        self._callbacks = []
        self._event = threading.Event()
        pass

    def set_result(self, result):
        self._result = result
        self._event.set()

        self._invoke_callbacks()

    def set_exception(self, exception):
        self._exception = exception
        self._event.set()
        self._invoke_callbacks()

    def result(self):
        self._event.wait()
        if self._exception:
            raise self._exception
        return self._result

    def done(self):
        return self._event.isSet()

    def running(self):
        return not self.done()

    def exception(self):
        self._event.wait()
        return self._exception

    def add_done_callback(self, callback):
        self._callbacks.append(callback)
        if self.done():
            self._invoke_cb(callback)

    def _invoke_callbacks(self):
        for callback in self._callbacks:
            self._invoke_cb(callback)

    def _invoke_cb(self, callback):
        try:
            callback(self)
        except:
            logging.exception("Exception when invoking callback")

    def continue_with(self, continuation_func):
        """
        Create a continuation that executes when the future is completed
        :param continuation_func: A function which takes the future as the only parameter. Return value of the function
        will be set as the result of the continuation future
        :return: A new future which will be completed when the continuation is done
        """
        future = Future()

        def callback(f):
            try:
                future.set_result(continuation_func(f))
            except:
                future.set_exception(sys.exc_info()[1])

        self.add_done_callback(callback)
        return future


class Invocation(object):
    handler = None
    sent_connection = None
    timer = None

    def __init__(self, request, partition_id=-1, address=None, connection=None, timeout=INVOCATION_TIMEOUT):
        self._event = threading.Event()
        self.timeout = timeout + time.time()
        self.address = address
        self.connection = connection
        self.partition_id = partition_id
        self.request = request
        self.future = Future()

    def has_connection(self):
        return self.connection is not None

    def has_handler(self):
        return self.handler is not None

    def has_partition_id(self):
        return self.partition_id >= 0

    def has_address(self):
        return self.address is not None

    def set_response(self, response):
        if self.timer:
            self.timer.cancel()
        self.future.set_result(response)

    def set_exception(self, exception):
        if self.timer:
            self.timer.cancel()
        self.future.set_exception(exception)

    def on_timeout(self):
        self.set_exception(RuntimeError("Requested timed out."))


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

        if message.get_message_type() == EXCEPTION_MESSAGE_TYPE:
            error = create_exception(ErrorCodec(message))
            return self.handle_exception(invocation, error)

        invocation.set_response(message)

    def handle_exception(self, invocation, error):
        self.logger.debug("Got exception for request %s: %s", invocation.request,
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
