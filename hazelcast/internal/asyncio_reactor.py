import asyncio
import io
import logging
import ssl
import time
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

    async def connection_factory(
        self, connection_manager, connection_id, address: Address, network_config, message_callback
    ):
        return await AsyncioConnection.create_and_connect(
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

    @classmethod
    async def create_and_connect(
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
        await this._create_connection(config, address)
        return this

    def _create_protocol(self):
        return HazelcastProtocol(self)

    async def _create_connection(self, config, address):
        ssl_context = None
        if config.ssl_enabled:
            ssl_context = self._create_ssl_context(config)
        server_hostname = None
        if config.ssl_check_hostname:
            server_hostname = address.host
        res = await self._loop.create_connection(
            self._create_protocol,
            host=self._address.host,
            port=self._address.port,
            ssl=ssl_context,
            server_hostname=server_hostname,
        )
        sock, self._proto = res
        if hasattr(sock, "_ssl_protocol"):
            sock = sock._ssl_protocol._transport._sock
        else:
            sock = sock._sock
        sockname = sock.getsockname()
        host, port = sockname[0], sockname[1]
        self.local_address = Address(host, port)

    def _write(self, buf):
        self._proto.write(buf)

    def _inner_close(self):
        self._proto.close()

    def _update_read_time(self, time):
        self.last_read_time = time

    def _update_write_time(self, time):
        self.last_write_time = time

    def _update_sent(self, sent):
        self._reactor.update_bytes_sent(sent)

    def _update_received(self, received):
        self._reactor.update_bytes_received(received)

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
            buf_size = max(sizehint, _BUFFER_SIZE)
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
