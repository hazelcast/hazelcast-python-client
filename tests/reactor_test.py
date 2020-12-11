import os
import socket
import threading
import time
from collections import OrderedDict

from mock import MagicMock
from parameterized import parameterized

from hazelcast import six
from hazelcast.config import _Config
from hazelcast.reactor import AsyncoreReactor, _WakeableLoop, _SocketedWaker, _PipedWaker, _BasicLoop, \
    AsyncoreConnection
from hazelcast.util import AtomicInteger
from tests.base import HazelcastTestCase


class ReactorTest(HazelcastTestCase):
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
    ("wakeable", _WakeableLoop,),
    ("basic", _BasicLoop,),
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
        loop.add_timer(float('inf'), callback)  # never expired, must be cleaned up
        time.sleep(1)
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

        loop.add_timer(float('inf'), callback)

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


class SocketedWakerTest(HazelcastTestCase):
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

        with self.assertRaises((IOError, socket.error)):  # BlockingIOError on Py3, socket.error on Py2
            waker._reader.recv(1)  # handle_read should consume the socket, there should be nothing

    def test_close(self):
        waker = self.waker
        writer = waker._writer
        reader = waker._reader
        self.assertNotEqual(-1, writer.fileno())
        self.assertNotEqual(-1, reader.fileno())

        waker.close()

        if six.PY3:
            self.assertEqual(-1, writer.fileno())
            self.assertEqual(-1, reader.fileno())
        else:
            # Closed sockets raise socket.error with EBADF error code in Python2
            with self.assertRaises(socket.error):
                writer.fileno()

            with self.assertRaises(socket.error):
                reader.fileno()


class PipedWakerTest(HazelcastTestCase):
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
            return  # pipes are not non-blocking on Windows, assertion below blocks forever on Windows

        with self.assertRaises((IOError, OSError)):  # BlockingIOError on Py3, OSError on Py2
            os.read(waker._read_fd, 1)  # handle_read should consume the pipe, there should be nothing

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


class AsyncoreConnectionTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc)
        cls.member = cls.cluster.start_member()

    @classmethod
    def tearDownClass(cls):
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    def test_socket_options(self):
        config = _Config()
        config.socket_options = [
            (socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ]
        conn = AsyncoreConnection(MagicMock(map=dict()), None, None, self.member.address, config, None)

        try:
            # By default this is set to 0
            self.assertEqual(1, conn.socket.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR))
        finally:
            conn._inner_close()

    def test_receive_buffer_size(self):
        # When the SO_RCVBUF option is set, we should try
        # to use that value while trying to read something.
        config = _Config()
        size = 64 * 1024
        config.socket_options = [
            (socket.SOL_SOCKET, socket.SO_RCVBUF, size)
        ]
        conn = AsyncoreConnection(MagicMock(map=dict()), None, None, self.member.address, config, None)

        try:
            # By default this is set to 128000
            self.assertEqual(size, conn.receive_buffer_size)
        finally:
            conn._inner_close()
