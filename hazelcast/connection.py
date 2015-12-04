from Queue import Queue
import socket, asyncore
from hazelcast.message import ClientMessageParser
import threading

#TODO: wrap this in something
event_queue = Queue()
def event_loop():
    while True:
        event = event_queue.get()
        event[0](event[1])
event_thread = threading.Thread(target=event_loop)
event_thread.setDaemon(True)
event_thread.start()

class ConnectionManager(object):
    def __init__(self):
        self._io_thread = None
        self.connections = {}
        pass

    def get_connection(self, address):
        print(self.connections)
        if address in self.connections:
            return self.connections[address]
        return None

    def get_or_connect(self, address, authenticator):
        if address in self.connections:
            return self.connections[address]

        connection = Connection(address)
        self._ensure_io()
        authenticator(connection)
        self.connections[connection.endpoint] = connection
        return connection

    def _ensure_io(self):
        if self._io_thread is None:
            self._io_thread = threading.Thread(target=self.io_loop)
            self._io_thread.daemon = True
            self._io_thread.start()

    def io_loop(self):
        asyncore.loop(count=None)
        print("IO Thread has exited.")


BUFFER_SIZE = 4096

class Connection(asyncore.dispatcher):
    def __init__(self, address):
        asyncore.dispatcher.__init__(self)
        self._address = address
        self._correlation_id = 0
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.endpoint = None
        print("Connecting to", address)
        self.connect(address)
        self._write_queue = Queue()
        self._write_queue.put("CB2")
        self._pending = {}
        self._listeners = {}

    def handle_connect(self):
        print("Connected to ", self._address)

    def handle_read(self):
        data = self.recv(BUFFER_SIZE)

        if len(data) == BUFFER_SIZE:  # TODO: more to read
            raise NotImplementedError()

        message = ClientMessageParser(data)
        correlation_id = message.get_correlation_id()
        if correlation_id not in self._pending:
            print("Got message with unknown correlation id", correlation_id)
            return

        if message.is_listener_message():
            print("Got event message")
            if correlation_id not in self._listeners:
                print("Got event message with unknown correlation id", correlation_id)
                return
            invocation = self._listeners[correlation_id]
            event_queue.put((invocation.handler, message))
            return

        invocation = self._pending[correlation_id]
        invocation.response = message
        self._pending.pop(correlation_id)
        invocation.event.set()

    def handle_write(self):
        self._initiate_send()

    def handle_close(self):
        print("handle_close")
        self.close()

    def writable(self):
        return not self._write_queue.empty()

    def _send(self, data):
        self._write_queue.put(data)
        self._initiate_send()

    def _initiate_send(self):
        item = self._write_queue.get_nowait()
        sent = self.send(item)
        print("Written " + str(sent) + " bytes")
        # TODO: check if everything was sent

    def send_and_receive(self, message):
        self.set_correlation_id(message)
        invocation = Invocation(message)
        self._invoke(invocation)
        if invocation.event.wait(120):
            return invocation.response
        raise RuntimeError("Request timed out")

    def _invoke(self, invocation):
        self._pending[invocation.message.get_correlation_id()] = invocation
        self._send(invocation.message.to_bytes())

    def start_listening(self, message, handler):
        invocation = Invocation(message)
        invocation.handler = handler
        correlation_id = self.set_correlation_id(message)
        self._listeners[correlation_id] = invocation
        self._invoke(invocation)
        if invocation.event.wait(120):
            return invocation.response

    def set_correlation_id(self, message):
        curr = self._correlation_id
        message.set_correlation_id(curr)
        self._correlation_id += 1
        return curr

class Invocation:
    def __init__(self, message):
        self.message = message
        self.event = threading.Event()
        self.response = None
        self.handler = None
