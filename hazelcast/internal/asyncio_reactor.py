import asyncio
import io
import logging
import time
from asyncio import AbstractEventLoop, transports

from hazelcast.internal.asyncio_connection import Connection
from hazelcast.core import Address

_BUFFER_SIZE = 128000


_logger = logging.getLogger(__name__)


class AsyncioReactor:
    def __init__(self, loop: AbstractEventLoop | None = None):
        self._is_live = False
        self._loop = loop or asyncio.get_running_loop()
        self._bytes_sent = 0
        self._bytes_received = 0

    def add_timer(self, delay, callback):
        return self._loop.call_later(delay, callback)

    def start(self):
        self._is_live = True

    def shutdown(self):
        if not self._is_live:
            return
        # TODO: cancel tasks

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
        if this._config.ssl_enabled:
            await this._create_ssl_connection()
        else:
            await this._create_connection()
        return this

    def _create_protocol(self):
        return HazelcastProtocol(self)

    async def _create_connection(self):
        loop = self._loop
        res = await loop.create_connection(
            self._create_protocol, host=self._address.host, port=self._address.port
        )
        _sock, self._proto = res

    async def _create_ssl_connection(self):
        raise NotImplementedError

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


class HazelcastProtocol(asyncio.BufferedProtocol):

    PROTOCOL_STARTER = b"CP2"

    def __init__(self, conn: AsyncioConnection):
        self._conn = conn
        self._transport: transports.BaseTransport | None = None
        self.start_time: float | None = None
        self._write_buf = io.BytesIO()
        self._write_buf_size = 0
        self._recv_buf = None
        self._alive = True

    def connection_made(self, transport: transports.BaseTransport):
        self._transport = transport
        self.start_time = time.time()
        self.write(self.PROTOCOL_STARTER)
        _logger.debug("Connected to %s", self._conn._address)
        self._conn._loop.call_soon(self._write_loop)

    def connection_lost(self, exc):
        self._alive = False
        self._conn._loop.create_task(self._conn.close_connection(str(exc), None))
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

    def eof_received(self):
        self._alive = False

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
