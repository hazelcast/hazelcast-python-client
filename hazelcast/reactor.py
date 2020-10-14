import asyncore
import errno
import logging
import select
import socket
import sys
import threading
import time

from collections import deque
from functools import total_ordering

from hazelcast import six
from hazelcast.config import SSLProtocol
from hazelcast.connection import Connection
from hazelcast.core import Address
from hazelcast.errors import HazelcastError
from hazelcast.future import Future
from hazelcast.six.moves import queue

try:
    import ssl
except ImportError:
    ssl = None

_logger = logging.getLogger(__name__)


class AsyncoreReactor(object):
    _thread = None
    _is_live = False

    def __init__(self):
        self._timers = queue.PriorityQueue()
        self._map = {}

    def start(self):
        self._is_live = True
        self._thread = threading.Thread(target=self._loop, name="hazelcast-reactor")
        self._thread.daemon = True
        self._thread.start()

    def _loop(self):
        _logger.debug("Starting Reactor Thread")
        Future._threading_locals.is_reactor_thread = True
        while self._is_live:
            try:
                asyncore.loop(count=1, timeout=0.01, map=self._map)
                self._check_timers()
            except select.error as err:
                # TODO: parse error type to catch only error "9"
                _logger.warning("Connection closed by server")
                pass
            except:
                _logger.exception("Error in Reactor Thread")
                # TODO: shutdown client
                return
        _logger.debug("Reactor Thread exited. %s" % self._timers.qsize())
        self._cleanup_all_timers()

    def _check_timers(self):
        now = time.time()
        while not self._timers.empty():
            try:
                timer = self._timers.queue[0][1]
            except IndexError:
                return

            if timer.check_timer(now):
                try:
                    self._timers.get_nowait()
                except queue.Empty:
                    pass
            else:
                return

    def add_timer_absolute(self, timeout, callback):
        timer = Timer(timeout, callback, self._cleanup_timer)
        self._timers.put_nowait((timer.end, timer))
        return timer

    def add_timer(self, delay, callback):
        return self.add_timer_absolute(delay + time.time(), callback)

    def shutdown(self):
        if not self._is_live:
            return

        self._is_live = False

        if self._thread is not threading.current_thread():
            self._thread.join()

        for connection in list(self._map.values()):
            try:
                connection.close(None, HazelcastError("Client is shutting down"))
            except OSError as connection:
                if connection.args[0] == socket.EBADF:
                    pass
                else:
                    raise
        self._map.clear()

    def connection_factory(self, connection_manager, connection_id, address, network_config, message_callback):
        return AsyncoreConnection(self._map, connection_manager, connection_id,
                                  address, network_config, message_callback)

    def _cleanup_timer(self, timer):
        try:
            self._timers.queue.remove((timer.end, timer))
        except ValueError:
            pass

    def _cleanup_all_timers(self):
        while not self._timers.empty():
            try:
                _, timer = self._timers.get_nowait()
                timer.timer_ended_cb()
            except queue.Empty:
                return


_BUFFER_SIZE = 128000


class AsyncoreConnection(Connection, asyncore.dispatcher):
    sent_protocol_bytes = False
    read_buffer_size = _BUFFER_SIZE

    def __init__(self, dispatcher_map, connection_manager, connection_id, address,
                 config, message_callback):
        asyncore.dispatcher.__init__(self, map=dispatcher_map)
        Connection.__init__(self, connection_manager, connection_id, message_callback)
        self.connected_address = address

        self._write_lock = threading.Lock()
        self._write_queue = deque()
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)

        timeout = config.connection_timeout
        if not timeout:
            timeout = six.MAXSIZE

        self.socket.settimeout(timeout)

        # set tcp no delay
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # set socket buffer
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, _BUFFER_SIZE)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, _BUFFER_SIZE)

        for socket_option in config.socket_options:
            if socket_option.option is socket.SO_RCVBUF:
                self.read_buffer_size = socket_option.value

            self.socket.setsockopt(socket_option.level, socket_option.option, socket_option.value)

        self.connect((address.host, address.port))

        if ssl and config.ssl_enabled:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)

            protocol = config.ssl_protocol

            # Use only the configured protocol
            try:
                if protocol != SSLProtocol.SSLv2:
                    ssl_context.options |= ssl.OP_NO_SSLv2
                if protocol != SSLProtocol.SSLv3:
                    ssl_context.options |= ssl.OP_NO_SSLv3
                if protocol != SSLProtocol.TLSv1:
                    ssl_context.options |= ssl.OP_NO_TLSv1
                if protocol != SSLProtocol.TLSv1_1:
                    ssl_context.options |= ssl.OP_NO_TLSv1_1
                if protocol != SSLProtocol.TLSv1_2:
                    ssl_context.options |= ssl.OP_NO_TLSv1_2
                if protocol != SSLProtocol.TLSv1_3:
                    ssl_context.options |= ssl.OP_NO_TLSv1_3
            except AttributeError:
                pass

            ssl_context.verify_mode = ssl.CERT_REQUIRED

            if config.ssl_cafile:
                ssl_context.load_verify_locations(config.ssl_cafile)
            else:
                ssl_context.load_default_certs()

            if config.ssl_certfile:
                ssl_context.load_cert_chain(config.ssl_certfile, config.ssl_keyfile, config.ssl_password)

            if config.ssl_ciphers:
                ssl_context.set_ciphers(config.ssl_ciphers)

            self.socket = ssl_context.wrap_socket(self.socket)

        # the socket should be non-blocking from now on
        self.socket.settimeout(0)

        self.local_address = Address(*self.socket.getsockname())

        self._write_queue.append(b"CP2")

    def handle_connect(self):
        self.start_time = time.time()
        _logger.debug("Connected to %s", self.connected_address)

    def handle_read(self):
        reader = self._reader
        while True:
            data = self.recv(self.read_buffer_size)
            reader.read(data)
            self.last_read_time = time.time()
            if len(data) < self.read_buffer_size:
                break

        if reader.length:
            reader.process()

    def handle_write(self):
        with self._write_lock:
            try:
                data = self._write_queue.popleft()
            except IndexError:
                return
            sent = self.send(data)
            self.last_write_time = time.time()
            self.sent_protocol_bytes = True
            if sent < len(data):
                self._write_queue.appendleft(data[sent:])

    def handle_close(self):
        _logger.warning("Connection closed by server")
        self.close(None, IOError("Connection closed by server"))

    def handle_error(self):
        error = sys.exc_info()[1]
        if sys.exc_info()[0] is socket.error:
            if error.errno != errno.EAGAIN and error.errno != errno.EDEADLK:
                _logger.exception("Received error")
                self.close(None, IOError(error))
        else:
            _logger.exception("Received unexpected error: %s" % error)

    def readable(self):
        return self.live and self.sent_protocol_bytes

    def _write(self, buf):
        # if write queue is empty, send the data right away, otherwise add to queue
        if len(self._write_queue) == 0 and self._write_lock.acquire(False):
            try:
                sent = self.send(buf)
                self.last_write_time = time.time()
                if sent < len(buf):
                    _logger.info("Adding to queue")
                    self._write_queue.appendleft(buf[sent:])
            finally:
                self._write_lock.release()
        else:
            self._write_queue.append(buf)

    def writable(self):
        return len(self._write_queue) > 0

    def _inner_close(self):
        asyncore.dispatcher.close(self)

    def __repr__(self):
        return "Connection(id=%s, live=%s, remote_address=%s)" % (self._id, self.live, self.remote_address)

    def __str__(self):
        return self.__repr__()


@total_ordering
class Timer(object):
    __slots__ = ("end", "timer_ended_cb", "timer_canceled_cb", "canceled")

    def __init__(self, end, timer_ended_cb, timer_canceled_cb):
        self.end = end
        self.timer_ended_cb = timer_ended_cb
        self.timer_canceled_cb = timer_canceled_cb
        self.canceled = False

    def __eq__(self, other):
        return self.end == other.end

    def __ne__(self, other):
        return not (self == other)

    def __lt__(self, other):
        return self.end < other.end

    def cancel(self):
        self.canceled = True
        self.timer_canceled_cb(self)

    def check_timer(self, now):
        if self.canceled:
            return True

        if now >= self.end:
            self.timer_ended_cb()
            return True

        return False
