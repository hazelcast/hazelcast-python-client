import asyncio
import logging
import typing
import uuid

from hazelcast.protocol.codec import sql_execute_codec, sql_fetch_codec, sql_close_codec
from hazelcast.internal.asyncio_invocation import Invocation, InvocationService
from hazelcast.serialization.compact import SchemaNotReplicatedError, SchemaNotFoundError
from hazelcast.sql import (
    SqlExpectedResultType,
    _TIMEOUT_NOT_SET,
    _DEFAULT_CURSOR_BUFFER_SIZE,
    _IteratorBase,
    SqlRow,
    _SqlQueryId,
    HazelcastSqlError,
    _SqlErrorCode,
    SqlRowMetadata,
    _ExecuteResponse,
    _SqlStatement,
)
from hazelcast.util import (
    to_millis,
    try_to_get_error_message,
)

_logger = logging.getLogger(__name__)


class SqlService:
    """A service to execute SQL statements.

    Warnings:

        In order to use this service, Jet engine must be enabled on the members
        and the ``hazelcast-sql`` module must be in the classpath of the
        members.

        If you are using the CLI, Docker image, or distributions to start
        Hazelcast members, then you don't need to do anything, as the above
        preconditions are already satisfied for such members.

        However, if you are using Hazelcast members in the embedded mode, or
        receiving errors saying that ``The Jet engine is disabled`` or
        ``Cannot execute SQL query because "hazelcast-sql" module is not in the
        classpath.`` while executing queries, enable the Jet engine following
        one of the instructions pointed out in the error message, or add the
        ``hazelcast-sql`` module to your member's classpath.

    **Overview**

    Hazelcast is currently able to execute distributed SQL queries using the
    following connectors:

    - IMap (to query data stored in a :class:`Map <hazelcast.proxy.map.Map>`)
    - Kafka
    - Files

    SQL statements are not atomic. *INSERT*/*SINK* can fail and commit part of
    the data.

    **Usage**

    Before you can access any object using SQL, a *mapping* has to be created.
    See the documentation for the ``CREATE MAPPING`` command.

    When a query is executed, an :class:`SqlResult` is returned. You may get
    row iterator from the result. The result must be closed at the end. The
    iterator will close the result automatically when it is exhausted given
    that no error is raised during the iteration. The code snippet below
    demonstrates a typical usage pattern: ::

        client = await HazelcastClient.create_and_start()
        result = await client.sql.execute("SELECT * FROM person")
        async for row in result:
            print(row.get_object("person_id"))
            print(row.get_object("name"))
            ...

    See the documentation of the :class:`SqlResult` for more information about
    different iteration methods.
    """

    def __init__(self, internal_sql_service):
        self._service = internal_sql_service

    async def execute(
        self,
        sql: str,
        *params: typing.Any,
        cursor_buffer_size: int = _DEFAULT_CURSOR_BUFFER_SIZE,
        timeout: float = _TIMEOUT_NOT_SET,
        expected_result_type: int = SqlExpectedResultType.ANY,
        schema: str = None
    ) -> "SqlResult":
        """Executes an SQL statement.

        Args:
            sql: SQL string.
            *params: Query parameters that will replace the placeholders at
                the server-side. You may define parameter placeholders in the
                query with the ``?`` character. For every placeholder, a
                parameter value must be provided.
            cursor_buffer_size: The cursor buffer size measured in the
                number of rows.

                When a statement is submitted for execution, a
                :class:`SqlResult` is returned as a result. When rows are ready
                to be consumed, they are put into an internal buffer of the
                cursor. This parameter defines the maximum number of rows in
                that buffer. When the threshold is reached, the backpressure
                mechanism will slow down the execution, possibly to a complete
                halt, to prevent out-of-memory.

                Only positive values are allowed.

                The default value is expected to work well for most workloads.
                A bigger buffer size may give you a slight performance boost
                for queries with large result sets at the cost of increased
                memory consumption.

                Defaults to ``4096``.
            timeout: The execution timeout in seconds.

                If the timeout is reached for a running statement, it will be
                cancelled forcefully.

                Zero value means no timeout. ``-1`` means that the value from
                the server-side config will be used. Other negative values are
                prohibited.

                Defaults to ``-1``.
            expected_result_type: The expected result type.
            schema: The schema name.

                The engine will try to resolve the non-qualified object
                identifiers from the statement in the given schema. If not
                found, the default search path will be used.

                The schema name is case sensitive. For example, ``foo`` and
                ``Foo`` are different schemas.

                The default value is ``None`` meaning only the default search
                path is used.

        Returns:
            The execution result.

        Raises:
            HazelcastSqlError: In case of execution error.
            AssertionError: If the ``sql`` parameter is not a string, the
                ``schema`` is not a string or ``None``, the ``timeout`` is not
                an integer or float, or the ``cursor_buffer_size`` is not an
                integer.
            ValueError: If the ``sql`` parameter is an empty string, the
                ``timeout`` is negative and not equal to ``-1``, the
                ``cursor_buffer_size`` is not positive.
            TypeError: If the ``expected_result_type`` does not equal to one of
                the values or names of the members of the
                :class:`SqlExpectedResultType`.
        """
        return await self._service.execute(
            sql, params, cursor_buffer_size, timeout, expected_result_type, schema
        )


class _AsyncIterator(_IteratorBase):
    """An iterator that produces infinite stream of Futures. It is the
    responsibility of the user to either call them in blocking fashion,
    or call ``next`` only if the current call to next did not raise
    ``StopAsyncIteration`` error (possibly with callback-based code).
    """

    def __aiter__(self):
        return self

    async def __anext__(self):
        has_next = await self._has_next()
        if not has_next:
            # Iterator is exhausted, raise this to inform the user.
            # If the user continues to call next, we will continuously
            # raise this.
            raise StopAsyncIteration

        row = self._get_current_row()
        self.position += 1
        return SqlRow(self.row_metadata, row)

    async def _has_next(self) -> bool:
        if self.position == self.row_count:
            # We exhausted the current page.
            if self.is_last:
                # This was the last page, no row left
                # on the server side.
                return False

            # It seems that there are some rows left on the server.
            # Fetch them, and then return.
            page_fut = await self.fetch_fn()
            page = await page_fut
            self.on_next_page(page)
            return await self._has_next()

        # There are some elements left in the current page.
        return True


class SqlResult(typing.AsyncIterable[SqlRow]):
    """SQL query result.

    Depending on the statement type it represents a stream of
    rows or an update count.

        result = await client.sql.execute("SELECT ...")
        async for row in result:
            # Process the row.
            print(row)

    Note that, iterators can be requested at most once per SqlResult.

    One can call :func:`close` method of a result object to
    release the resources associated with the result on the server side.
    It might also be used to cancel query execution on the server side
    if it is still active.

    When the blocking API is used, one might also use ``async with``
    statement to automatically close the query even if an exception
    is thrown in the iteration. ::

        async with await client.sql.execute("SELECT ...").result() as result:
            async for row in result:
                # Process the row.
                print(row)


    To get the number of rows updated by the query, use the
    :func:`update_count`. ::

        result = await client.sql.execute("UPDATE ...")
        update_count = result.update_count()

    One does not have to call :func:`close` in this case, because the result
    will already be closed in the server-side.
    """

    def __init__(self, sql_service, connection, query_id, cursor_buffer_size, execute_response):
        self._sql_service = sql_service
        self._connection = connection
        self._query_id = query_id
        self._cursor_buffer_size = cursor_buffer_size
        self._lock = asyncio.Lock()
        self._execute_response = execute_response
        self._iterator_requested = False
        self._closed = self._is_closed(execute_response)
        self._fetch_task: asyncio.Task | None = None
        self._fetch_future: asyncio.Future | None = None

    def iterator(self) -> typing.AsyncIterator[SqlRow]:
        """Returns the iterator over the result rows.

        The iterator may be requested only once.

        Raises:
            ValueError: If the result only contains an update count, or the
                iterator is already requested.

        Returns:
            Iterator that produces Future of :class:`SqlRow` s. See the class
            documentation for the correct way to use this.
        """
        return self._get_iterator()

    def is_row_set(self) -> bool:
        """Returns whether this result has rows to iterate."""
        # By design, if the row_metadata (or row_page) is None,
        # we only got the update count.
        return self._execute_response.row_metadata is not None

    def update_count(self) -> int:
        """Returns the number of rows updated by the statement or ``-1`` if
        this result is a row set. In case the result doesn't contain rows but
        the update count isn't applicable or known, ``0`` is returned.
        """
        # This will be set to -1, when we got row set on the client side.
        # See _on_execute_response.
        return self._execute_response.update_count

    def get_row_metadata(self) -> SqlRowMetadata:
        """Gets the row metadata.

        Raises:
            ValueError: If the result only contains an update count.
        """

        response = self._execute_response
        if not response.row_metadata:
            raise ValueError("This result contains only update count")

        return response.row_metadata

    async def close(self) -> None:
        """Release the resources associated with the query result.

        The query engine delivers the rows asynchronously. The query may
        become inactive even before all rows are consumed. The invocation
        of this command will cancel the execution of the query on all members
        if the query is still active. Otherwise it is no-op. For a result
        with an update count it is always no-op.

        The returned Future results with:

        - :class:`HazelcastSqlError`: In case there is an error closing the
          result.
        """

        async with self._lock:
            if self._closed:
                # Do nothing if the result is already closed.
                return None

            error = HazelcastSqlError(
                self._sql_service.get_client_id(),
                _SqlErrorCode.CANCELLED_BY_USER,
                "Query was cancelled by the user",
                None,
            )
            if not self._fetch_future:
                # Make sure that all subsequent fetches will fail.
                # XXX:
                self._fetch_future = asyncio.Future()

            self._on_fetch_error_unsafe(error)
            self._closed = True
            # Send the close request
            try:
                await self._sql_service.close(self._connection, self._query_id)
            except Exception as e:
                # If the close request is failed somehow,
                # wrap it in a HazelcastSqlError.
                raise self._sql_service.re_raise(e, self._connection)

    def __aiter__(self):
        return self._get_iterator()

    def _get_iterator(self):
        response = self._execute_response
        if not response.row_metadata:
            # Can't get an iterator when we only have update count
            raise ValueError("This result contains only update count")

        if self._iterator_requested:
            # Can't get an iterator when we already get one
            raise ValueError("Iterator can be requested only once")

        self._iterator_requested = True
        iterator = _AsyncIterator(
            response.row_metadata,
            self._fetch_next_page,
        )
        # Pass the first page information to the iterator
        iterator.on_next_page(response.row_page)
        return iterator

    async def _fetch_next_page(self):
        # Fetches the next page, if there is no fetch request in-flight.
        async with self._lock:
            if self._fetch_future:
                # A fetch request is already in-flight, return it.
                return self._fetch_future

            future = asyncio.Future()
            self._fetch_future = future
            self._fetch_task = asyncio.create_task(self._handle_fetch_response())
            return future

    async def _handle_fetch_response(self):
        # Handles the result of the fetch request, by either:
        # - setting it to exception, so that the future calls to
        #  fetch fails immediately.
        # - setting it to next page, and setting self._fetch_future
        #  to None so that the next fetch request might actually
        # try to fetch something from the server.
        try:
            response = await self._sql_service.fetch(
                self._connection, self._query_id, self._cursor_buffer_size
            )
            response_error = response["error"]
            if response_error:
                # There is a server side error sent to client.
                sql_error = HazelcastSqlError(
                    response_error.originating_member_uuid,
                    response_error.code,
                    response_error.message,
                    None,
                    response_error.suggestion,
                )
                await self._on_fetch_error(sql_error)
                return

            # The result contains the next page, as expected.
            await self._on_fetch_response(response["row_page"])
        except Exception as e:
            # Something went bad, we couldn't get response from
            # the server, invocation failed.
            await self._on_fetch_error(self._sql_service.re_raise(e, self._connection))

    async def _on_fetch_error(self, error):
        # Sets the fetch future with exception, but not resetting it so that the next fetch request fails immediately.
        async with self._lock:
            self._on_fetch_error_unsafe(error)

    def _on_fetch_error_unsafe(self, error):
        # Sets the fetch future with exception, but not resetting it so that the next fetch request fails immediately.
        self._fetch_future.set_exception(error)

    async def _on_fetch_response(self, page):
        # Sets the fetch future with the next page, resets it, and if this is the last page, marks the result as closed.
        async with self._lock:
            future = self._fetch_future
            self._fetch_future = None
            if page.is_last:
                # This is the last page, there is nothing
                # more on the server.
                self._closed = True

            # Resolving the future before resetting self._fetch_future
            # might result in an infinite loop for non-blocking iterators
            future.set_result(page)

    @staticmethod
    def _is_closed(execute_response):
        # Returns whether the result is already closed or not.
        #
        # Result might be closed if the first response
        #
        # - contains the last page of the rowset (single page rowset)
        # - contains just the update count
        return (
            execute_response.row_metadata is None  # Just an update count
            or execute_response.row_page.is_last  # Single page result
        )

    async def __aenter__(self):
        # The response for the execute request is already
        # received. There is nothing more to do.
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        # Ignoring the possible exception details
        # since we close the query regardless of that.
        await self.close()


class _InternalSqlService:
    """Internal SQL service that offers more public API
    than the one exposed to the user.
    """

    def __init__(
        self,
        connection_manager,
        serialization_service,
        invocation_service: InvocationService,
        send_schema_and_retry_fn,
    ):
        self._connection_manager = connection_manager
        self._serialization_service = serialization_service
        self._invocation_service = invocation_service
        self._send_schema_and_retry_fn = send_schema_and_retry_fn

    async def execute(
        self, sql, params, cursor_buffer_size, timeout, expected_result_type, schema
    ) -> "SqlResult":
        """Constructs a statement and executes it.

        Args:
            sql (str): SQL string.
            params (tuple): Query parameters.
            cursor_buffer_size (int): Cursor buffer size.
            timeout (float): Timeout of the query.
            expected_result_type (SqlExpectedResultType): Expected result type
                of the query.
            schema (str or None): The schema name.

        Returns:
            SqlResult: The execution result.
        """
        statement = _SqlStatement(
            sql, params, cursor_buffer_size, timeout, expected_result_type, schema
        )
        connection = None
        try:
            try:
                # Serialize the passed parameters.
                serialized_params = [
                    self._serialization_service.to_data(param) for param in statement.parameters
                ]
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry_fn(
                    e,
                    self.execute,
                    sql,
                    params,
                    cursor_buffer_size,
                    timeout,
                    expected_result_type,
                    schema,
                )

            connection = self._get_query_connection()
            # Create a new, unique query id.
            query_id = _SqlQueryId.from_uuid(connection.remote_uuid)
            request = sql_execute_codec.encode_request(
                statement.sql,
                serialized_params,
                # to_millis expects None to produce -1
                to_millis(None if statement.timeout == -1 else statement.timeout),
                statement.cursor_buffer_size,
                statement.schema,
                statement.expected_result_type,
                query_id,
                False,
            )
            invocation = Invocation(
                request,
                connection=connection,
                response_handler=lambda m: sql_execute_codec.decode_response(m, self._to_object),
            )
            res = await self._invocation_service.ainvoke(invocation)
            return SqlResult(
                self,
                connection,
                query_id,
                statement.cursor_buffer_size,
                self._handle_execute_response(res),
            )
        except Exception as e:
            raise self.re_raise(e, connection)

    async def fetch(self, connection, query_id, cursor_buffer_size):
        """Fetches the next page of the query execution.

        Args:
            connection (hazelcast.connection.Connection): Connection
                that the first execute request, hence the fetch request
                must route to.
            query_id (_SqlQueryId): Unique id of the query.
            cursor_buffer_size (int): Size of cursor buffer. Same as
                the one used in the first execute request.
        """
        request = sql_fetch_codec.encode_request(query_id, cursor_buffer_size)
        invocation = Invocation(
            request,
            connection=connection,
            response_handler=lambda m: sql_fetch_codec.decode_response(m, self._to_object),
        )
        return await self._invocation_service.ainvoke(invocation)

    def get_client_id(self):
        """
        Returns:
            uuid.UUID: Unique client UUID.
        """
        return self._connection_manager.client_uuid

    def re_raise(self, error, connection):
        """Returns the error wrapped as the :class:`HazelcastSqlError`
        so that it can be raised to the user.

        Args:
            error (Exception): The error to reraise.
            connection (hazelcast.connection.Connection|None): Connection
                that the query requests are routed to. If it is not
                live, we will inform the user about the possible
                cluster topology change.

        Returns:
            HazelcastSqlError: The reraised error.
        """
        if connection and not connection.live:
            return HazelcastSqlError(
                self.get_client_id(),
                _SqlErrorCode.CONNECTION_PROBLEM,
                "Cluster topology changed while a query was executed: Member cannot be reached: %s"
                % connection.remote_address,
                error,
            )

        if isinstance(error, HazelcastSqlError):
            return error

        return HazelcastSqlError(
            self.get_client_id(), _SqlErrorCode.GENERIC, try_to_get_error_message(error), error
        )

    async def close(self, connection, query_id) -> None:
        """Closes the remote query cursor.

        Args:
            connection (hazelcast.connection.Connection): Connection
                that the first execute request, hence the close request
                must route to.
            query_id (_SqlQueryId): The query id to close.

        Returns:
            None
        """
        request = sql_close_codec.encode_request(query_id)
        invocation = Invocation(request, connection=connection)
        await self._invocation_service.ainvoke(invocation)

    def _to_object(self, data):
        try:
            return self._serialization_service.to_object(data)
        except SchemaNotFoundError as e:
            raise e
        except Exception as e:
            raise HazelcastSqlError(
                self.get_client_id(),
                _SqlErrorCode.GENERIC,
                "Failed to deserialize query result value: %s" % try_to_get_error_message(e),
                e,
            )

    def _get_query_connection(self):
        try:
            connection = self._connection_manager.get_random_connection_for_sql()
        except Exception as e:
            raise self.re_raise(e, None)

        if not connection:
            raise HazelcastSqlError(
                self.get_client_id(),
                _SqlErrorCode.CONNECTION_PROBLEM,
                "Client is not connected",
                None,
            )

        return connection

    def _handle_execute_response(self, response):
        response_error = response["error"]
        if response_error:
            # There is a server-side error sent to the client.
            sql_error = HazelcastSqlError(
                response_error.originating_member_uuid,
                response_error.code,
                response_error.message,
                None,
                response_error.suggestion,
            )
            raise sql_error

        row_metadata = response["row_metadata"]
        if row_metadata is not None:
            # The result contains some rows, not an update count.
            row_metadata = SqlRowMetadata(row_metadata)
            # Set the update count to -1.
            return _ExecuteResponse(row_metadata, response["row_page"], -1)

        # Result only contains the update count.
        return _ExecuteResponse(None, None, response["update_count"])
