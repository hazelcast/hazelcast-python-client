import asyncore
import errno
import logging
import select
import socket
import sys
import threading
import time
from Queue import PriorityQueue
from collections import deque

from hazelcast.connection import Connection, BUFFER_SIZE
from hazelcast.exception import HazelcastError
from hazelcast.future import Future


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
        self.logger.debug("Starting Reactor Thread")
        Future._threading_locals.is_reactor_thread = True
        while self._is_live:
            try:
                asyncore.loop(count=1000, timeout=0.01, map=self._map)
                self._check_timers()
            except select.error as err:
                # TODO: parse error type to catch only error "9"
                self.logger.warn("Connection closed by server.")
                pass
            except:
                self.logger.exception("Error in Reactor Thread")
                # TODO: shutdown client
                return
        self.logger.debug("Reactor Thread exited.")

    def _check_timers(self):
        now = time.time()
        while not self._timers.empty():
            try:
                _, timer = self._timers.queue[0]
            except IndexError:
                return

            if timer.check_timer(now):
                self._timers.get_nowait()
            else:
                return

    def add_timer_absolute(self, timeout, callback):
        timer = Timer(timeout, callback, self._cleanup_timer)
        self._timers.put_nowait((timer.end, timer))
        return timer

    def add_timer(self, delay, callback):
        return self.add_timer_absolute(delay + time.time(), callback)

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

    def new_connection(self, address, connect_timeout, socket_options, connection_closed_callback, message_callback):
        return AsyncoreConnection(self._map, address, connect_timeout, socket_options, connection_closed_callback,
                                  message_callback)

    def _cleanup_timer(self, timer):
        try:
            self._timers.queue.remove((timer.end, timer))
        except ValueError:
            pass


class AsyncoreConnection(Connection, asyncore.dispatcher):
    sent_protocol_bytes = False

    def __init__(self, map, address, connect_timeout, socket_options, connection_closed_callback, message_callback):
        asyncore.dispatcher.__init__(self, map=map)
        Connection.__init__(self, address, connection_closed_callback, message_callback)

        self._write_lock = threading.Lock()
        self._write_queue = deque()
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(connect_timeout)

        # set tcp no delay
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # set socket buffer
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFFER_SIZE)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFFER_SIZE)

        for socket_option in socket_options:
            self.socket.setsockopt(socket_option.level, socket_option.option, socket_option.value)

        self.connect(self._address)

        # the socket should be non-blocking from now on
        self.socket.settimeout(0)

        self._write_queue.append("CB2")

    def handle_connect(self):
        self.logger.debug("Connected to %s", self._address)

    def handle_read(self):
        self._read_buffer += self.recv(BUFFER_SIZE)
        self.receive_message()

    def handle_write(self):
        with self._write_lock:
            try:
                data = self._write_queue.popleft()
            except IndexError:
                return
            sent = self.send(data)
            self.sent_protocol_bytes = True
            if sent < len(data):
                self._write_queue.appendleft(data[sent:])

    def handle_close(self):
        self.logger.warn("Connection closed by server.")
        self.close(IOError("Connection closed by server."))

    def handle_error(self):
        error = sys.exc_info()[1]
        if error.errno != errno.EAGAIN and error.errno != errno.EDEADLK:
            self.logger.exception("Received error")
            self.close(IOError(error))

    def readable(self):
        return not self._closed and self.sent_protocol_bytes

    def write(self, data):
        # if write queue is empty, send the data right away, otherwise add to queue
        if len(self._write_queue) == 0 and self._write_lock.acquire(False):
            try:
                sent = self.send(data)
                if sent < len(data):
                    self.logger.info("adding to queue")
                    self._write_queue.appendleft(data[sent:])
            finally:
                self._write_lock.release()
        else:
            self._write_queue.append(data)

    def writable(self):
        return len(self._write_queue) > 0

    def close(self, cause):
        if not self._closed:
            self._closed = True
            asyncore.dispatcher.close(self)
            self._connection_closed_callback(self, cause)


class Timer(object):
    canceled = False

    def __init__(self, end, timer_ended_cb, timer_canceled_cb):
        self.end = end
        self.timer_ended_cb = timer_ended_cb
        self.timer_canceled_cb = timer_canceled_cb

    def cancel(self):
        self.canceled = True
        self.timer_canceled_cb(self)

    def check_timer(self, now):
        if self.canceled:
            return True

        if now > self.end:
            self.timer_ended_cb()
            return True
