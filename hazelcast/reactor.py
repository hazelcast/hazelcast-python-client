import asyncore
import logging
import socket
import threading
import time
from Queue import PriorityQueue
from collections import deque

from hazelcast.connection import Connection, BUFFER_SIZE


class AsyncoreReactor(object):
    _thread = None
    _is_live = False
    logger = logging.getLogger("Reactor")

    def __init__(self):
        self._timers = PriorityQueue()

    def start(self):
        self._is_live = True
        self._thread = threading.Thread(target=self._loop, name="hazelcast-reactor")
        self._thread.daemon = True
        self._thread.start()

    def _loop(self):
        self.logger.debug("Starting IO Thread")
        while self._is_live:
            try:
                asyncore.loop(count=10000, timeout=0.1)
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
        asyncore.close_all()
        self._is_live = False
        self._thread.join()


class AsyncoreConnection(Connection, asyncore.dispatcher):
    def __init__(self, address, connection_closed_cb):
        asyncore.dispatcher.__init__(self)
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
        self.logger.debug("Connection closed by server.")
        self.close_connection()

    def handle_error(self):
        self.logger.exception("Received error")
        self.close_connection()

    def readable(self):
        return not self._closed

    def write(self, data):
        self._write_queue.append(data)

    def close_connection(self):
        self._closed = True
        self.close()
        self._connection_closed_cb(self)


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
