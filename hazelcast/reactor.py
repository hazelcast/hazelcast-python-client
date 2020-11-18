import asyncore
import errno
import logging
import os
import select
import socket
import sys
import threading
import time

from collections import deque
from functools import total_ordering
from heapq import heappush, heappop

from hazelcast import six
from hazelcast.config import SSLProtocol
from hazelcast.connection import Connection
from hazelcast.core import Address
from hazelcast.errors import HazelcastError
from hazelcast.future import Future

try:
    import ssl
except ImportError:
    ssl = None

try:
    import fcntl
except ImportError:
    fcntl = None

try:
    from _thread import get_ident
except ImportError:
    # Python2
    from thread import get_ident

_logger = logging.getLogger(__name__)


def _set_nonblocking(fd):
    if not fcntl:
        return

    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


class _SocketAdapter(object):
    def __init__(self, fd):
        self._fd = fd

    def fileno(self):
        return self._fd

    def close(self):
        os.close(self._fd)

    def getsockopt(self, level, optname, buflen=None):
        if level == socket.SOL_SOCKET and optname == socket.SO_ERROR and not buflen:
            return 0
        raise NotImplementedError("Only asyncore specific behaviour is implemented.")


class _AbstractWaker(asyncore.dispatcher):
    def __init__(self, map):
        asyncore.dispatcher.__init__(self, map=map)
        self.awake = False

    def writable(self):
        return False

    def wake(self):
        raise NotImplementedError("wake")


class _PipedWaker(_AbstractWaker):
    def __init__(self, map):
        _AbstractWaker.__init__(self, map)
        self._read_fd, self._write_fd = os.pipe()
        self.set_socket(_SocketAdapter(self._read_fd))
        _set_nonblocking(self._read_fd)
        _set_nonblocking(self._write_fd)

    def wake(self):
        if not self.awake:
            self.awake = True
            try:
                os.write(self._write_fd, b"x")
            except (IOError, ValueError):
                pass

    def handle_read(self):
        self.awake = False
        try:
            while len(os.read(self._read_fd, 4096)) == 4096:
                pass
        except (IOError, OSError):
            pass

    def close(self):
        _AbstractWaker.close(self)   # Will close the reader
        os.close(self._write_fd)


class _SocketedWaker(_AbstractWaker):
    def __init__(self, map):
        _AbstractWaker.__init__(self, map)
        self._writer = socket.socket()
        self._writer.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        a = socket.socket()
        a.bind(("127.0.0.1", 0))
        a.listen(1)
        addr = a.getsockname()

        try:
            self._writer.connect(addr)
            self._reader, _ = a.accept()
        finally:
            a.close()

        self.set_socket(self._reader)
        self._writer.settimeout(0)
        self._reader.settimeout(0)

    def wake(self):
        if not self.awake:
            self.awake = True
            try:
                self._writer.send(b"x")
            except (IOError, socket.error, ValueError):
                pass

    def handle_read(self):
        self.awake = False
        try:
            while len(self._reader.recv(4096)) == 4096:
                pass
        except (IOError, socket.error):
            pass

    def close(self):
        _AbstractWaker.close(self)  # Will close the reader
        self._writer.close()


class _AbstractLoop(object):
    def __init__(self, map):
        self._map = map
        self._timers = []  # Accessed only from the reactor thread
        self._new_timers = deque()  # Popped only from the reactor thread
        self._is_live = False
        self._thread = None
        self._ident = -1

    def start(self):
        self._is_live = True
        self._thread = threading.Thread(target=self._loop, name="hazelcast-reactor")
        self._thread.daemon = True
        self._thread.start()
        self._ident = self._thread.ident

    def _loop(self):
        _logger.debug("Starting Reactor Thread")
        Future._threading_locals.is_reactor_thread = True
        while self._is_live:
            try:
                self.run_loop()
                self._check_timers()
            except select.error:
                # TODO: parse error type to catch only error "9"
                _logger.warning("Connection closed by server")
                pass
            except:
                _logger.exception("Error in Reactor Thread")
                # TODO: shutdown client
                return
        _logger.debug("Reactor Thread exited")
        self._cleanup_all_timers()

    def add_timer(self, delay, callback):
        timer = Timer(delay + time.time(), callback)
        self._new_timers.append((timer.end, timer))
        return timer

    def _check_timers(self):
        timers = self._timers

        if self._new_timers:
            new_timers = self._new_timers
            while new_timers:
                # There is no need to check for exception here,
                # reactor thread is the only one popping from
                # the deque. So, if the we pass the size check
                # above, there should be at least one element
                heappush(timers, new_timers.popleft())

        if timers:
            now = time.time()
            while timers:
                timer = timers[0][1]
                if timer.check_timer(now):
                    heappop(timers)
                else:
                    # Timer in the root of the min heap is not expired.
                    # Therefore, there should be no expired
                    # timers in the heap.
                    return

    def _cleanup_all_timers(self):
        timers = self._timers
        new_timers = self._new_timers

        while timers:
            _, timer = timers.pop()
            timer.timer_ended_cb()

        # Although it is not the case with the current code base,
        # the timers ended above may add new timers. So, the order
        # is important.
        while new_timers:
            _, timer = new_timers.popleft()
            timer.timer_ended_cb()

    def check_loop(self):
        raise NotImplementedError("check_loop")

    def run_loop(self):
        raise NotImplementedError("run_loop")

    def wake_loop(self):
        raise NotImplementedError("wake_loop")

    def shutdown(self):
        raise NotImplementedError("shutdown")


class _WakeableLoop(_AbstractLoop):
    _waker_class = _PipedWaker if os.name != 'nt' else _SocketedWaker

    def __init__(self, map):
        _AbstractLoop.__init__(self, map)
        self.waker = self._waker_class(map)

    def check_loop(self):
        assert not self.waker.awake
        self.wake_loop()
        assert self.waker.awake
        self.run_loop()
        assert not self.waker.awake

    def run_loop(self):
        asyncore.loop(timeout=0.01, use_poll=True, map=self._map, count=1)

    def wake_loop(self):
        if self._ident != get_ident():
            self.waker.wake()

    def shutdown(self):
        if not self._is_live:
            return

        self._is_live = False

        if self._ident != get_ident():
            self._thread.join()

        for connection in list(self._map.values()):
            if connection is self.waker:
                continue

            try:
                connection.close(None, HazelcastError("Client is shutting down"))
            except OSError as connection:
                if connection.args[0] == socket.EBADF:
                    pass
                else:
                    raise

        self.waker.close()
        self._map.clear()


class _BasicLoop(_AbstractLoop):
    def check_loop(self):
        pass

    def run_loop(self):
        asyncore.loop(timeout=0.001, use_poll=True, map=self._map, count=1)

    def wake_loop(self):
        pass

    def shutdown(self):
        if not self._is_live:
            return

        self._is_live = False

        if self._ident != get_ident():
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


class AsyncoreReactor(object):
    def __init__(self):
        self.map = {}
        loop = None
        try:
            loop = _WakeableLoop(self.map)
            loop.check_loop()
        except:
            _logger.exception("Failed to initialize the wakeable loop. "
                              "Using the basic loop instead. "
                              "When used in the blocking mode, client"
                              "may have sub-optimal performance.")
            if loop:
                loop.shutdown()
            loop = _BasicLoop(self.map)
        self._loop = loop

    def start(self):
        self._loop.start()

    def add_timer(self, delay, callback):
        return self._loop.add_timer(delay, callback)

    def wake_loop(self):
        self._loop.wake_loop()

    def shutdown(self):
        self._loop.shutdown()

    def connection_factory(self, connection_manager, connection_id, address, network_config, message_callback):
        return AsyncoreConnection(self, connection_manager, connection_id, address, network_config, message_callback)


_BUFFER_SIZE = 128000


class AsyncoreConnection(Connection, asyncore.dispatcher):
    sent_protocol_bytes = False
    read_buffer_size = _BUFFER_SIZE

    def __init__(self, reactor, connection_manager, connection_id, address,
                 config, message_callback):
        asyncore.dispatcher.__init__(self, map=reactor.map)
        Connection.__init__(self, connection_manager, connection_id, message_callback)

        self._reactor = reactor
        self.connected_address = address
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
        while True:
            try:
                data = self._write_queue.popleft()
            except IndexError:
                return

            sent = self.send(data)
            self.last_write_time = time.time()
            self.sent_protocol_bytes = True
            if sent < len(data):
                self._write_queue.appendleft(data[sent:])

            if sent == 0:
                return

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
            _logger.exception("Received unexpected error: %s", error)

    def readable(self):
        return self.live and self.sent_protocol_bytes

    def _write(self, buf):
        self._write_queue.append(buf)
        self._reactor.wake_loop()

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
    __slots__ = ("end", "timer_ended_cb", "canceled")

    def __init__(self, end, timer_ended_cb):
        self.end = end
        self.timer_ended_cb = timer_ended_cb
        self.canceled = False

    def __eq__(self, other):
        return self.end == other.end

    def __ne__(self, other):
        return self.end != other.end

    def __lt__(self, other):
        return self.end < other.end

    def cancel(self):
        self.canceled = True

    def check_timer(self, now):
        if self.canceled:
            return True

        if now >= self.end:
            self.timer_ended_cb()
            return True

        return False
