import socket
from hazelcast.message import ClientMessageParser

class ConnectionManager(object):
    _connections = {}

    def __init__(self):
        pass

    def get_or_connect(self, address, authenticator):
        if address not in self._connections:
            connection = Connection(address)
            authenticator(connection)
            self._connections[address] = connection
        return self._connections[address]

BUFFER_SIZE = 4096

class Connection(object):
    def __init__(self, address):
        adr = tuple(address.split(":"))
        print("Connecting to ", adr)
        self._correlation_id = 0
        self._socket = socket.create_connection(adr)
        self._send("CB2")

    def _send(self, bytes):
        sent = self._socket.send(bytes)
        print("Sent ", sent, " bytes")

    def _receive(self):
        return self._socket.recv(BUFFER_SIZE)

    def send_and_receive(self, message):
        message.set_correlation_id(self._correlation_id)
        self._correlation_id += 1
        self._send(message.to_bytes())
        received = self._receive()
        return ClientMessageParser(received)