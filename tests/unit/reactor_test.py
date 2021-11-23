import os
import socket
import threading
import unittest
from collections import OrderedDict

from mock import MagicMock
from parameterized import parameterized

from hazelcast.config import _Config
from hazelcast.core import Address
from hazelcast.reactor import (
    AsyncoreReactor,
    _WakeableLoop,
    _SocketedWaker,
    _PipedWaker,
    _BasicLoop,
    AsyncoreConnection,
)
from hazelcast.util import AtomicInteger
from tests.base import HazelcastTestCase
from tests.util import get_current_timestamp


class ReactorTest(unittest.TestCase):
    def test_default_loop_is_wakeable(self):
        reactor = AsyncoreReactor()
        self.assertIsInstance(reactor._loop, _WakeableLoop)

    def test_reactor_lifetime(self):
        t_count = threading.active_count()
        reactor = AsyncoreReactor()
        reactor.start()
        try:
            self.assertEqual(t_count + 1, threading.active_count())  # reactor thread
        finally:
            reactor.shutdown()
        self.assertEqual(t_count, threading.active_count())


LOOP_CLASSES = [
    (
        "wakeable",
        _WakeableLoop,
    ),
    (
        "basic",
        _BasicLoop,
    ),
]


class LoopTest(HazelcastTestCase):
    def test_wakeable_loop_default_waker(self):
        loop = _WakeableLoop({})
        try:
            if os.name == "nt":
                self.assertIsInstance(loop.waker, _SocketedWaker)
            else:
                self.assertIsInstance(loop.waker, _PipedWaker)
        finally:
            loop.waker.close()

    def test_wakeable_loop_waker_closes_last(self):
        dispatchers = OrderedDict()
        loop = _WakeableLoop(dispatchers)  # Waker comes first in the dict

        mock_dispatcher = MagicMock(readable=lambda: False, writeable=lambda: False)
        dispatchers[loop.waker._fileno + 1] = mock_dispatcher

        original_close = loop.waker.close

        def assertion():
            mock_dispatcher.close.assert_called()
            original_close()

        loop.waker.close = assertion

        loop.shutdown()

    @parameterized.expand(LOOP_CLASSES)
    def test_check_loop(self, _, cls):
        loop = cls({})
        # For the WakeableLoop, we are checking that
        # the loop can be waken up, and once the reactor
        # handles the written bytes, it is not awake
        # anymore. Assertions are in the method
        # implementation. For, the BasicLoop, this should
        # be no-op, just checking it is not raising any
        # error.
        loop.check_loop()

    @parameterized.expand(LOOP_CLASSES)
    def test_add_timer(self, _, cls):
        call_count = AtomicInteger()

        def callback():
            call_count.add(1)

        loop = cls({})
        loop.start()
        loop.add_timer(0, callback)  # already expired, should be run immediately

        def assertion():
            self.assertEqual(1, call_count.get())

        try:
            self.assertTrueEventually(assertion)
        finally:
            loop.shutdown()

    @parameterized.expand(LOOP_CLASSES)
    def test_timer_cleanup(self, _, cls):
        call_count = AtomicInteger()

        def callback():
            call_count.add(1)

        loop = cls({})
        loop.start()
        loop.add_timer(float("inf"), callback)  # never expired, must be cleaned up
        try:
            self.assertEqual(0, call_count.get())
        finally:
            loop.shutdown()

        def assertion():
            self.assertEqual(1, call_count.get())

        self.assertTrueEventually(assertion)

    @parameterized.expand(LOOP_CLASSES)
    def test_timer_that_adds_another_timer(self, _, cls):
        loop = cls({})
        loop.start()

        call_count = AtomicInteger()

        def callback():
            if call_count.get() == 0:
                loop.add_timer(0, callback)
            call_count.add(1)

        loop.add_timer(float("inf"), callback)

        loop.shutdown()

        def assertion():
            self.assertEqual(2, call_count.get())  # newly added timer must also be cleaned up

        self.assertTrueEventually(assertion)

    @parameterized.expand(LOOP_CLASSES)
    def test_timer_that_shuts_down_loop(self, _, cls):
        # It may be the case that, we want to shutdown the client(hence, the loop) in timers
        loop = cls({})
        loop.start()

        loop.add_timer(0, lambda: loop.shutdown())

        def assertion():
            self.assertFalse(loop._is_live)

        try:
            self.assertTrueEventually(assertion)
        finally:
            loop.shutdown()  # Should be no op


class SocketedWakerTest(unittest.TestCase):
    def setUp(self):
        self.waker = _SocketedWaker({})

    def tearDown(self):
        try:
            self.waker.close()
        except:
            pass

    def test_wake(self):
        waker = self.waker
        self.assertFalse(waker.awake)
        waker.wake()
        self.assertTrue(waker.awake)
        self.assertEqual(b"x", waker._reader.recv(1))

    def test_wake_while_awake(self):
        waker = self.waker
        waker.wake()
        waker.wake()
        self.assertTrue(waker.awake)
        self.assertEqual(b"x", waker._reader.recv(2))  # only the first one should write

    def test_handle_read(self):
        waker = self.waker
        waker.wake()
        self.assertTrue(waker.awake)
        waker.handle_read()
        self.assertFalse(waker.awake)

        # BlockingIOError on Py3, socket.error on Py2
        with self.assertRaises((IOError, socket.error)):
            # handle_read should consume the socket, there should be nothing
            waker._reader.recv(1)

    def test_close(self):
        waker = self.waker
        writer = waker._writer
        reader = waker._reader
        self.assertNotEqual(-1, writer.fileno())
        self.assertNotEqual(-1, reader.fileno())

        waker.close()

        self.assertEqual(-1, writer.fileno())
        self.assertEqual(-1, reader.fileno())


class PipedWakerTest(unittest.TestCase):
    def setUp(self):
        self.waker = _PipedWaker({})

    def tearDown(self):
        try:
            self.waker.close()
        except:
            pass

    def test_wake(self):
        waker = self.waker
        self.assertFalse(waker.awake)
        waker.wake()
        self.assertTrue(waker.awake)
        self.assertEqual(b"x", os.read(waker._read_fd, 1))

    def test_wake_while_awake(self):
        waker = self.waker
        waker.wake()
        waker.wake()
        self.assertTrue(waker.awake)
        self.assertEqual(b"x", os.read(waker._read_fd, 2))  # only the first one should write

    def test_handle_read(self):
        waker = self.waker
        waker.wake()
        self.assertTrue(waker.awake)
        waker.handle_read()
        self.assertFalse(waker.awake)

        if os.name == "nt":
            # pipes are not non-blocking on Windows, assertion below blocks forever on Windows
            return

        # BlockingIOError on Py3, OSError on Py2
        with self.assertRaises((IOError, OSError)):
            # handle_read should consume the pipe, there should be nothing
            os.read(waker._read_fd, 1)

    def test_close(self):
        waker = self.waker
        w_fd = waker._write_fd
        r_fd = waker._read_fd
        self.assertEqual(1, os.write(w_fd, b"x"))
        self.assertEqual(b"x", os.read(r_fd, 1))

        waker.close()

        with self.assertRaises(OSError):
            os.write(w_fd, b"x")

        with self.assertRaises(OSError):
            os.read(r_fd, 1)


class MockServer(object):
    def __init__(self):
        self._s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._s.settimeout(3.0)
        self._s.bind(("localhost", 0))
        self._s.listen(1)
        self._t = threading.Thread(target=self._handler)
        self._t.start()

    def _handler(self):
        try:
            conn, _ = self._s.accept()
            conn.close()
        except:
            pass

    def get_address(self):
        host, port = self._s.getsockname()
        return Address(host, port)

    def close(self):
        self._s.close()
        self._t.join()


class AsyncoreConnectionTest(unittest.TestCase):
    def setUp(self):
        self.server = None

    def tearDown(self):
        if self.server:
            self.server.close()

    def test_socket_options(self):
        self.server = MockServer()
        config = _Config()
        config.socket_options = [(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)]

        conn = AsyncoreConnection(
            MagicMock(map=dict()), None, None, self.server.get_address(), config, None
        )

        try:
            # By default this is set to 0
            self.assertEqual(1, conn.socket.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR))
        finally:
            conn._inner_close()

    def test_receive_buffer_size(self):
        # When the SO_RCVBUF option is set, we should try
        # to use that value while trying to read something.
        self.server = MockServer()
        config = _Config()
        size = 64 * 1024
        config.socket_options = [(socket.SOL_SOCKET, socket.SO_RCVBUF, size)]
        conn = AsyncoreConnection(
            MagicMock(map=dict()), None, None, self.server.get_address(), config, None
        )

        try:
            # By default this is set to 128000
            self.assertEqual(size, conn.receive_buffer_size)
        finally:
            conn._inner_close()

    def test_send_buffer_size(self):
        # When the SO_SNDBUF option is set, we should try
        # to use that value while trying to write something.
        self.server = MockServer()
        config = _Config()
        size = 64 * 1024
        config.socket_options = [(socket.SOL_SOCKET, socket.SO_SNDBUF, size)]
        conn = AsyncoreConnection(
            MagicMock(map=dict()), None, None, self.server.get_address(), config, None
        )

        try:
            # By default this is set to 128000
            self.assertEqual(size, conn.send_buffer_size)
        finally:
            conn._inner_close()

    def test_constructor_with_unreachable_addresses(self):
        addr = Address("192.168.0.1", 5701)
        config = _Config()
        start = get_current_timestamp()
        conn = AsyncoreConnection(MagicMock(map=dict()), MagicMock(), None, addr, config, None)
        try:
            # Server is unreachable, but this call should return
            # before connection timeout
            self.assertLess(get_current_timestamp() - start, config.connection_timeout)
        finally:
            conn.close(None, None)

    def test_resources_cleaned_up_after_immediate_failure(self):
        addr = Address("invalid-address", 5701)
        config = _Config()
        mock_reactor = MagicMock(map={})
        try:
            conn = AsyncoreConnection(mock_reactor, MagicMock(), None, addr, config, None)
            conn.close(None, None)
            self.fail("Connection attempt to an invalid address should fail immediately")
        except socket.error:
            # Constructor of the connection should remove itself from the
            # dispatchers map of the reactor.
            self.assertEqual(0, len(mock_reactor.map))
