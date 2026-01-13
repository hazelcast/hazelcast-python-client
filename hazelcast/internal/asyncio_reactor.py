import asyncio
import errno
import io
import logging
import os
import socket
import ssl
import time
from errno import errorcode
from asyncio import AbstractEventLoop, transports

from hazelcast.config import Config, SSLProtocol
from hazelcast.internal.asyncio_connection import Connection
from hazelcast.core import Address

_BUFFER_SIZE = 128000


_logger = logging.getLogger(__name__)


class AsyncioReactor:
    def __init__(self, loop: AbstractEventLoop | None = None):
        self._loop = loop or asyncio.get_running_loop()
        self._bytes_sent = 0
        self._bytes_received = 0

    def add_timer(self, delay, callback):
        return self._loop.call_later(delay, callback)

    def connection_factory(
        self, connection_manager, connection_id, address: Address, network_config, message_callback
    ):
        return AsyncioConnection.create_and_connect(
            self._loop,
            self,
            connection_manager,
            connection_id,
            address,
            network_config,
            message_callback,
        )

    def update_bytes_sent(self, sent: int):
        self._bytes_sent += sent

    def update_bytes_received(self, received: int):
        self._bytes_received += received


class AsyncioConnection(Connection):
    def __init__(
        self,
        loop,
        reactor: AsyncioReactor,
        connection_manager,
        connection_id,
        address,
        config,
        message_callback,
    ):
        super().__init__(connection_manager, connection_id, message_callback)
        self._loop = loop
        self._reactor = reactor
        self._address = address
        self._config = config
        self._proto = None
        self.connected_address = address
        self._preconn_buffers: list = []
        self._create_task: asyncio.Task | None = None
        self._close_task: asyncio.Task | None = None
        self._connected = False
        self._receive_buffer_size = _BUFFER_SIZE
        self._sock = None

    @classmethod
    def create_and_connect(
        cls,
        loop,
        reactor: AsyncioReactor,
        connection_manager,
        connection_id,
        address,
        config,
        message_callback,
    ):
        this = cls(
            loop, reactor, connection_manager, connection_id, address, config, message_callback
        )
        this._create_task = asyncio.create_task(this._create_connection(config, address))
        if config.connection_timeout > 0:
            this._close_task = asyncio.create_task(this._close_timer_cb(config.connection_timeout))
        return this

    def _create_protocol(self):
        return HazelcastProtocol(self)

    async def _create_connection(self, config, address):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        sock.settimeout(0)
        self._set_socket_options(sock, config)
        server_hostname = None
        ssl_context = None
        if config.ssl_enabled:
            server_hostname = address.host
            ssl_context = self._create_ssl_context(config)

        try:
            self.connect(sock, (address.host, address.port))
        except socket.error as e:
            self._inner_close()
            raise e

        self._sock = sock

        res = await self._loop.create_connection(
            self._create_protocol,
            ssl=ssl_context,
            server_hostname=server_hostname,
            sock=sock,
        )
        self._connected = True

        try:
            sock.getpeername()
        except OSError as err:
            if err.errno not in (errno.ENOTCONN, errno.EINVAL):
                raise
            self._connected = False

        sock, self._proto = res
        sock = sock.get_extra_info("socket")
        sockname = sock.getsockname()
        host, port = sockname[0], sockname[1]
        self.local_address = Address(host, port)
        self._connect_timer_task = None
        if not self._connected:
            self._connect_timer_task = self._loop.create_task(
                self._connect_retry_cb(0.01, self._sock, (address.host, address.port))
            )

    async def _connect_retry_cb(self, timeout, sock, address):
        await asyncio.sleep(timeout)
        if self._connected and self._close_task:
            self._close_task.cancel()
            return
        try:
            self.connect(sock, address)
        except Exception:
            # close task will handle closing the connection
            return
        if not self._connected:
            self._connect_timer_task = self._loop.create_task(
                self._connect_retry_cb(timeout, sock, address)
            )
        elif self._close_task:
            self._close_task.cancel()

    def connect(self, sock, address):
        self._connected = False
        err = sock.connect_ex(address)
        if (
            err in (errno.EINPROGRESS, errno.EALREADY, errno.EWOULDBLOCK)
            or err == errno.EINVAL
            and os.name == "nt"
        ):
            return
        if err in (0, errno.EISCONN):
            self.handle_connect_event(sock)
        else:
            raise OSError(err, errorcode[err])

    def handle_connect_event(self, sock):
        err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if err != 0:
            raise OSError(err, _strerror(err))
        self.handle_connect()

    def handle_connect(self):
        self._connected = True
        # write any data that were buffered before the socket is available
        if self._preconn_buffers:
            for b in self._preconn_buffers:
                self._proto.write(b)
            self._preconn_buffers.clear()
        if self._close_task:
            self._close_task.cancel()

        self.start_time = time.time()
        _logger.debug("Connected to %s", self.connected_address)

    async def _close_timer_cb(self, timeout):
        await asyncio.sleep(timeout)
        if not self._connected:
            if self._connect_timer_task:
                self._connect_timer_task.cancel()
            await self.close_connection(None, IOError("Connection timed out"))

    def _write(self, buf):
        if not self._proto:
            self._preconn_buffers.append(buf)
            return
        self._proto.write(buf)

    def _inner_close(self):
        if self._close_task:
            self._close_task.cancel()
        if self._proto:
            self._proto.close()
        self._connected = False
        if self._sock:
            try:
                self._sock.close()
            except OSError as why:
                if why.errno not in (errno.ENOTCONN, errno.EBADF):
                    raise

    def _update_read_time(self, time):
        self.last_read_time = time

    def _update_write_time(self, time):
        self.last_write_time = time

    def _update_sent(self, sent):
        self._reactor.update_bytes_sent(sent)

    def _update_received(self, received):
        self._reactor.update_bytes_received(received)

    def _set_socket_options(self, sock, config):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, _BUFFER_SIZE)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, _BUFFER_SIZE)
        for level, option_name, value in config.socket_options:
            if option_name is socket.SO_RCVBUF:
                self._receive_buffer_size = value

            sock.setsockopt(level, option_name, value)

    def _create_ssl_context(self, config: Config):
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
            ssl_context.load_cert_chain(
                config.ssl_certfile, config.ssl_keyfile, config.ssl_password
            )
        if config.ssl_ciphers:
            ssl_context.set_ciphers(config.ssl_ciphers)
        if config.ssl_check_hostname:
            ssl_context.check_hostname = True

        return ssl_context


class HazelcastProtocol(asyncio.BufferedProtocol):

    PROTOCOL_STARTER = b"CP2"

    def __init__(self, conn: AsyncioConnection):
        self._conn = conn
        self._transport: transports.BaseTransport | None = None
        self.start_time: float | None = None
        self._write_buf = io.BytesIO()
        self._write_buf_size = 0
        self._recv_buf = None
        # asyncio tasks are weakly referenced
        # storing tasks here in order not to lose them midway
        # see: https: // docs.python.org / 3 / library / asyncio - task.html  # creating-tasks
        self._tasks: set = set()

    def connection_made(self, transport: transports.BaseTransport):
        self._transport = transport
        self.start_time = time.time()
        self.write(self.PROTOCOL_STARTER)
        _logger.debug("Connected to %s", self._conn._address)
        self._conn._loop.call_soon(self._write_loop)

    def connection_lost(self, exc):
        _logger.warning("Connection closed by server")
        task = self._conn._loop.create_task(
            self._conn.close_connection(None, IOError("Connection closed by server"))
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return False

    def close(self):
        self._transport.close()

    def write(self, buf):
        self._write_buf.write(buf)
        self._write_buf_size += len(buf)

    def get_buffer(self, sizehint):
        if self._recv_buf is None:
            buf_size = max(sizehint, self._conn._receive_buffer_size)
            self._recv_buf = memoryview(bytearray(buf_size))
        return self._recv_buf

    def buffer_updated(self, nbytes):
        recv_bytes = self._recv_buf[:nbytes]
        self._conn._update_read_time(time.time())
        self._conn._update_received(nbytes)
        self._conn._reader.read(recv_bytes)
        if self._conn._reader.length:
            self._conn._reader.process()

    def _do_write(self):
        if not self._write_buf_size:
            return
        buf_bytes = self._write_buf.getvalue()
        self._transport.write(buf_bytes[: self._write_buf_size])
        self._conn._update_write_time(time.time())
        self._conn._update_sent(self._write_buf_size)
        self._write_buf.seek(0)
        self._write_buf_size = 0

    def _write_loop(self):
        self._do_write()
        return self._conn._loop.call_later(0.01, self._write_loop)


def _strerror(err):
    try:
        return os.strerror(err)
    except (ValueError, OverflowError, NameError):
        if err in errorcode:
            return errorcode[err]
        return "Unknown error %s" % err
