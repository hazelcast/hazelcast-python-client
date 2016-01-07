import asyncore
import logging
import socket
import threading
import time
from Queue import PriorityQueue
from collections import deque

import sys

from hazelcast.connection import Connection, BUFFER_SIZE
from hazelcast.exception import HazelcastError


class AsyncoreReactor(object):
    _thread = None
    _is_live = False
    logger = logging.getLogger("Reactor")

    def __init__(self):
        self._timers = PriorityQueue()
        self._map = {}

    def start(self):
        self._is_live = True
        self._thread = threading.Thread(target=self._loop, name="hazelcast-reactor")
        self._thread.daemon = True
        self._thread.start()

    def _loop(self):
        self.logger.debug("Starting IO Thread")
        while self._is_live:
            try:
                asyncore.loop(count=10000, timeout=0.1, map=self._map)
                self._check_timers()
            except:
                self.logger.exception("Error in IO Thread")
                return
        self.logger.debug("IO Thread exited.")

    def _check_timers(self):
        now = time.time()
        while not self._timers.empty():
            _, timer = self._timers.queue[0]  # TODO: possible race condition
            if timer.check_timer(now):
                self._timers.get_nowait()
            else:
                return

    def add_timer_absolute(self, timeout, callback):
        timer = Timer(timeout, callback)
        self._timers.put_nowait((timer.end, timer))
        return timer

    def add_timer(self, delay, callback):
        self.add_timer_absolute(delay + time.time(), callback)

    def shutdown(self):
        for connection in self._map.values():
            try:
                connection.close(HazelcastError("Client is shutting down"))
            except OSError, connection:
                if connection.args[0] == socket.EBADF:
                    pass
                else:
                    raise
        self._map.clear()
        self._is_live = False
        self._thread.join()

    def new_connection(self, address, callback):
        return AsyncoreConnection(self._map, address, callback)

class AsyncoreConnection(Connection, asyncore.dispatcher):
    def __init__(self, map, address, connection_closed_cb):
        asyncore.dispatcher.__init__(self, map=map)
        Connection.__init__(self, address, connection_closed_cb)

        self._write_queue = deque()
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect(self._address)
        self.write("CB2")

    def handle_connect(self):
        self.logger.debug("Connected to %s", self._address)

    def handle_read(self):
        self._read_buffer += self.recv(BUFFER_SIZE)
        self.receive_message()

    def handle_write(self):
        try:
            item = self._write_queue.popleft()
        except IndexError:
            return
        sent = self.send(item)
        if sent < len(item):
            self._write_queue.appendleft(item[sent:])

    def handle_close(self):
        self.logger.warn("Connection closed by server.")
        self.close(IOError("Connection closed by server."))

    def handle_error(self):
        self.logger.exception("Received error")
        self.close(IOError(sys.exc_info()[1]))

    def readable(self):
        return not self._closed

    def write(self, data):
        self._write_queue.append(data)

    def close(self, cause):
        self._closed = True
        asyncore.dispatcher.close(self)
        self._connection_closed_cb(self, cause)


class Timer(object):
    canceled = False

    def __init__(self, end, timer_ended_cb):
        self.end = end
        self.timer_ended_cb = timer_ended_cb

    def cancel(self):
        self.canceled = True

    def check_timer(self, now):
        if self.canceled:
            return True

        if now > self.end:
            self.timer_ended_cb()
            return True
