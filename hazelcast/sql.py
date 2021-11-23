import logging
import uuid
from threading import RLock

from hazelcast.errors import HazelcastError
from hazelcast.future import Future, ImmediateFuture, ImmediateExceptionFuture
from hazelcast.invocation import Invocation
from hazelcast.util import (
    UUIDUtil,
    to_millis,
    check_true,
    get_attr_name,
    try_to_get_error_message,
    check_is_number,
    check_is_int,
    try_to_get_enum_value,
)

_logger = logging.getLogger(__name__)


class SqlService(object):
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

        client = hazelcast.HazelcastClient()

        result = client.sql.execute("SELECT * FROM person").result()

        for row in result:
            print(row.get_object("person_id"))
            print(row.get_object("name"))
            ...

    See the documentation of the :class:`SqlResult` for more information about
    different iteration methods.
    """

    def __init__(self, internal_sql_service):
        self._service = internal_sql_service

    def execute(self, sql, *params, **kwargs):
        """Executes an SQL statement.

        Args:
            sql (str): SQL string.
            *params: Query parameters that will replace the placeholders at
                the server-side. You may define parameter placeholders in the
                query with the ``?`` character. For every placeholder, a
                parameter value must be provided.

        Keyword Args:
            cursor_buffer_size (int): The cursor buffer size measured in the
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
            timeout (float or int): The execution timeout in seconds.

                If the timeout is reached for a running statement, it will be
                cancelled forcefully.

                Zero value means no timeout. ``-1`` means that the value from
                the server-side config will be used. Other negative values are
                prohibited.

                Defaults to ``-1``.
            expected_result_type (SqlExpectedResultType): The expected result
                type.
            schema (str or None): The schema name.

                The engine will try to resolve the non-qualified object
                identifiers from the statement in the given schema. If not
                found, the default search path will be used.

                The schema name is case sensitive. For example, ``foo`` and
                ``Foo`` are different schemas.

                The default value is ``None`` meaning only the default search
                path is used.

        Returns:
            hazelcast.future.Future[SqlResult]: The execution result.

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
        return self._service.execute(sql, params, kwargs)


class _SqlQueryId(object):
    """Cluster-wide unique query ID."""

    __slots__ = ("member_id_high", "member_id_low", "local_id_high", "local_id_low")

    def __init__(self, member_id_high, member_id_low, local_id_high, local_id_low):
        self.member_id_high = member_id_high
        """int: Most significant bits of the UUID of the member
        that the query will route to.
        """

        self.member_id_low = member_id_low
        """int: Least significant bits of the UUID of the member
        that the query will route to."""

        self.local_id_high = local_id_high
        """int: Most significant bits of the UUID of the local id."""

        self.local_id_low = local_id_low
        """int: Least significant bits of the UUID of the local id."""

    @classmethod
    def from_uuid(cls, member_uuid):
        """Generates a local random UUID and creates a query id
        out of it and the given member UUID.

        Args:
            member_uuid (uuid.UUID): UUID of the member.

        Returns:
            _SqlQueryId: Generated unique query id.
        """
        local_id = uuid.uuid4()

        member_msb, member_lsb = UUIDUtil.to_bits(member_uuid)
        local_msb, local_lsb = UUIDUtil.to_bits(local_id)

        return cls(member_msb, member_lsb, local_msb, local_lsb)


class SqlColumnMetadata(object):
    """Metadata of a column in an SQL row."""

    __slots__ = ("_name", "_type", "_nullable")

    def __init__(self, name, column_type, nullable, is_nullable_exists):
        self._name = name
        self._type = column_type
        self._nullable = nullable if is_nullable_exists else True

    @property
    def name(self):
        """str: Name of the column."""
        return self._name

    @property
    def type(self):
        """SqlColumnType: Type of the column."""
        return self._type

    @property
    def nullable(self):
        """bool: ``True`` if this column values can be ``None``, ``False``
        otherwise.
        """
        return self._nullable

    def __repr__(self):
        return "%s %s" % (self.name, get_attr_name(SqlColumnType, self.type))


class _SqlError(object):
    """Server-side error that is propagated to the client."""

    __slots__ = ("code", "message", "originating_member_uuid", "suggestion")

    def __init__(self, code, message, originating_member_uuid, _, suggestion):
        self.code = code
        """_SqlErrorCode: The error code."""

        self.message = message
        """str: The error message."""

        self.originating_member_uuid = originating_member_uuid
        """uuid.UUID: UUID of the member that caused or initiated an error condition."""

        self.suggestion = suggestion
        """str: Suggested SQL statement to remediate experienced error."""


class _SqlPage(object):
    """A finite set of rows returned to the user."""

    __slots__ = ("_column_types", "_columns", "_is_last")

    def __init__(self, column_types, columns, last):
        self._column_types = column_types
        self._columns = columns
        self._is_last = last

    @property
    def row_count(self):
        """int: Number of rows in the page."""
        # Each column should have equal number of rows.
        # Just check the first one.
        return len(self._columns[0])

    @property
    def column_count(self):
        """int: Number of columns."""
        return len(self._column_types)

    @property
    def is_last(self):
        """bool: Whether this is the last page or not."""
        return self._is_last

    def get_value(self, column_index, row_index):
        """
        Args:
            column_index (int):
            row_index (int):

        Returns:
            The value with the given indexes.
        """
        return self._columns[column_index][row_index]


class SqlColumnType(object):

    VARCHAR = 0
    """
    Represented by ``str``.
    """

    BOOLEAN = 1
    """
    Represented by ``bool``.
    """

    TINYINT = 2
    """
    Represented by ``int``.
    """

    SMALLINT = 3
    """
    Represented by ``int``.
    """

    INTEGER = 4
    """
    Represented by ``int``.
    """

    BIGINT = 5
    """
    Represented by ``int``.
    """

    DECIMAL = 6
    """
    Represented by ``decimal.Decimal``.
    """

    REAL = 7
    """
    Represented by ``float``.
    """

    DOUBLE = 8
    """
    Represented by ``float``. 
    """

    DATE = 9
    """
    Represented by ``datetime.date``.
    """

    TIME = 10
    """
    Represented by ``datetime.time``.
    """

    TIMESTAMP = 11
    """
    Represented by ``datetime.datetime`` with ``None`` ``tzinfo``.
    """

    TIMESTAMP_WITH_TIME_ZONE = 12
    """
    Represented by ``datetime.datetime`` with ``non-None`` ``tzinfo``.
    """

    OBJECT = 13
    """
    Could be represented by any Python class.
    """

    NULL = 14
    """
    The type of the generic SQL ``NULL`` literal.
    
    The only valid value of ``NULL`` type is ``None``.
    """


class _SqlErrorCode(object):

    GENERIC = -1
    """
    Generic error.
    """

    CONNECTION_PROBLEM = 1001
    """
    A network connection problem between members, or between a client and a member.
    """

    CANCELLED_BY_USER = 1003
    """
    Query was cancelled due to user request.
    """

    TIMEOUT = 1004
    """
    Query was cancelled due to timeout.
    """

    PARTITION_DISTRIBUTION = 1005
    """
    A problem with partition distribution.
    """

    MAP_LOADING_IN_PROGRESS = 1007
    """
    Map loading is not finished yet.
    """

    PARSING = 1008
    """
    Generic parsing error.
    """

    INDEX_INVALID = 1009
    """
    An error caused by an attempt to query an index that is not valid.
    """

    OBJECT_NOT_FOUND = 1010
    """
    Object (mapping/table) not found.
    """

    DATA_EXCEPTION = 2000
    """
    An error with data conversion or transformation.
    """


class HazelcastSqlError(HazelcastError):
    """Represents an error occurred during the SQL query execution."""

    def __init__(self, originating_member_uuid, code, message, cause, suggestion=None):
        super(HazelcastSqlError, self).__init__(message, cause)
        self._originating_member_uuid = originating_member_uuid
        self._suggestion = suggestion

        # TODO: This is private API, might be good to make it public or
        #  remove this information altogether.
        self._code = code

    @property
    def originating_member_uuid(self):
        """uuid.UUID: UUID of the member that caused or initiated an error condition."""
        return self._originating_member_uuid

    @property
    def suggestion(self):
        """str: Suggested SQL statement to remediate experienced error."""
        return self._suggestion


class SqlRowMetadata(object):
    """Metadata for the returned rows."""

    __slots__ = ("_columns", "_name_to_index")

    COLUMN_NOT_FOUND = -1
    """Constant indicating that the column is not found."""

    def __init__(self, columns):
        self._columns = columns
        self._name_to_index = {column.name: index for index, column in enumerate(columns)}

    @property
    def columns(self):
        """list[SqlColumnMetadata]: List of column metadata."""
        return self._columns

    @property
    def column_count(self):
        """int: Number of columns in the row."""
        return len(self._columns)

    def get_column(self, index):
        """
        Args:
            index (int): Zero-based column index.

        Returns:
            SqlColumnMetadata: Metadata for the given column index.

        Raises:
            IndexError: If the index is out of bounds.
            AssertionError: If the index is not an integer.
        """
        check_is_int(index, "Index must an integer")
        return self._columns[index]

    def find_column(self, column_name):
        """
        Args:
            column_name (str): Name of the column.

        Returns:
            int: Column index or :const:`COLUMN_NOT_FOUND` if a column
            with the given name is not found.

        Raises:
            AssertionError: If the column name is not a string.
        """
        check_true(isinstance(column_name, str), "Column name must be a string")
        return self._name_to_index.get(column_name, SqlRowMetadata.COLUMN_NOT_FOUND)

    def __repr__(self):
        return "[%s]" % ", ".join(
            "%s %s" % (column.name, get_attr_name(SqlColumnType, column.type))
            for column in self._columns
        )


class SqlRow(object):
    """One of the rows of an SQL query result.

    The columns of the rows can be retrieved using

    - :func:`get_object` with column name.
    - :func:`get_object_with_index` with column index.

    Apart from these methods, the row objects can also be treated as a ``dict``
    or ``list`` and columns can be retrieved using the ``[]`` operator.

    If an integer value is passed to the ``[]`` operator, it will implicitly
    call the :func:`get_object_with_index` and return the result.

    For any other type passed into the the ``[]`` operator, :func:`get_object`
    will be called. Note that, :func:`get_object` expects ``str`` values.
    Hence, the ``[]`` operator will raise error for any type other than integer
    and string.
    """

    __slots__ = ("_row_metadata", "_row", "_deserialize_fn")

    def __init__(self, row_metadata, row, deserialize_fn):
        self._row_metadata = row_metadata
        self._row = row
        self._deserialize_fn = deserialize_fn

    def get_object(self, column_name):
        """Gets the value in the column indicated by the column name.

        Column name should be one of those defined in :class:`SqlRowMetadata`,
        case-sensitive. You may also use :func:`SqlRowMetadata.find_column` to
        test for column existence.

        The type of the returned value depends on the SQL type of the column.
        No implicit conversions are performed on the value.

        Warnings:

            Each call to this method might result in a deserialization if the
            column type for this object is :const:`SqlColumnType.OBJECT`.
            It is advised to assign the result of this method call to some
            variable and reuse it.

        Args:
            column_name (str):

        Returns:
            Value of the column.

        Raises:
            ValueError: If a column with the given name does not exist.
            AssertionError: If the column name is not a string.
            HazelcastSqlError: If the object cannot be deserialized.

        See Also:
            :attr:`metadata`

            :func:`SqlRowMetadata.find_column`

            :attr:`SqlColumnMetadata.type`

            :attr:`SqlColumnMetadata.name`
        """
        index = self._row_metadata.find_column(column_name)
        if index == SqlRowMetadata.COLUMN_NOT_FOUND:
            raise ValueError("Column '%s' doesn't exist" % column_name)
        return self._deserialize_fn(self._row[index])

    def get_object_with_index(self, column_index):
        """Gets the value of the column by index.

        The class of the returned value depends on the SQL type of the column.
        No implicit conversions are performed on the value.

        Warnings:

            Each call to this method might result in a deserialization if the
            column type for this object is :const:`SqlColumnType.OBJECT`.
            It is advised to assign the result of this method call to some
            variable and reuse it.

        Args:
            column_index (int): Zero-based column index.

        Returns:
            Value of the column.

        Raises:
            IndexError: If the column index is out of bounds.
            AssertionError: If the column index is not an integer.
            HazelcastSqlError: If the object cannot be deserialized.

        See Also:
            :attr:`metadata`

            :attr:`SqlColumnMetadata.type`
        """
        check_is_int(column_index, "Column index must be an integer")
        return self._deserialize_fn(self._row[column_index])

    @property
    def metadata(self):
        """SqlRowMetadata: The row metadata."""
        return self._row_metadata

    def __getitem__(self, item):
        if isinstance(item, int):
            return self.get_object_with_index(item)

        return self.get_object(item)

    def __repr__(self):
        return "[%s]" % ", ".join(
            "%s %s=%s"
            % (
                self._row_metadata.get_column(i).name,
                get_attr_name(SqlColumnType, self._row_metadata.get_column(i).type),
                self.get_object_with_index(i),
            )
            for i in range(self._row_metadata.column_count)
        )


class _ExecuteResponse(object):
    """Represent the response of the first execute request."""

    __slots__ = ("row_metadata", "row_page", "update_count")

    def __init__(self, row_metadata, row_page, update_count):
        self.row_metadata = row_metadata
        """SqlRowMetadata: Row metadata or None, if the response only 
        contains update count."""

        self.row_page = row_page
        """_SqlPage: First page of the query response or None, if the
        response only contains update count.
        """

        self.update_count = update_count
        """int: Update count or -1 if the result is a rowset."""


class _IteratorBase(object):
    """Base class for the blocking and Future-producing
    iterators to use."""

    __slots__ = (
        "row_metadata",
        "fetch_fn",
        "deserialize_fn",
        "page",
        "row_count",
        "position",
        "is_last",
    )

    def __init__(self, row_metadata, fetch_fn, deserialize_fn):
        self.row_metadata = row_metadata
        """SqlRowMetadata: Row metadata."""

        self.fetch_fn = fetch_fn
        """function: Fetches the next page. It produces a Future[_SqlPage]."""

        self.deserialize_fn = deserialize_fn
        """function: Deserializes the value."""

        self.page = None
        """_SqlPage: Current page."""

        self.row_count = 0
        """int: Number of rows in the current page."""

        self.position = 0
        """int: Index of the next row in the page."""

        self.is_last = False
        """bool: Whether this is the last page or not."""

    def on_next_page(self, page):
        """
        Called when a new page is fetched or on the
        initialization of the iterator to update its
        internal state.

        Args:
            page (_SqlPage):
        """
        self.page = page
        self.row_count = page.row_count
        self.is_last = page.is_last
        self.position = 0

    def _get_current_row(self):
        """
        Returns:
            list: The row pointed by the current position.
        """

        # Deserialization happens lazily while getting the object.
        return [self.page.get_value(i, self.position) for i in range(self.page.column_count)]


class _FutureProducingIterator(_IteratorBase):
    """An iterator that produces infinite stream of Futures. It is the
    responsibility of the user to either call them in blocking fashion,
    or call ``next`` only if the current call to next did not raise
    ``StopIteration`` error (possibly with callback-based code).
    """

    def __iter__(self):
        return self

    def __next__(self):
        return self._has_next().continue_with(self._has_next_continuation)

    def _has_next_continuation(self, future):
        """Based on the call to :func:`_has_next`, either
        raises ``StopIteration`` error or gets the current row
        and returns it.

        Args:
            future (hazelcast.future.Future):

        Returns:
            SqlRow:
        """
        has_next = future.result()
        if not has_next:
            # Iterator is exhausted, raise this to inform the user.
            # If the user continues to call next, we will continuously
            # raise this.
            raise StopIteration

        row = self._get_current_row()
        self.position += 1
        return SqlRow(self.row_metadata, row, self.deserialize_fn)

    def _has_next(self):
        """Returns a Future indicating whether there are more rows
        left to iterate.

        Returns:
            hazelcast.future.Future:
        """
        if self.position == self.row_count:
            # We exhausted the current page.

            if self.is_last:
                # This was the last page, no row left
                # on the server side.
                return ImmediateFuture(False)

            # It seems that there are some rows left on the server.
            # Fetch them, and then return.
            return self.fetch_fn().continue_with(self._fetch_continuation)

        # There are some elements left in the current page.
        return ImmediateFuture(True)

    def _fetch_continuation(self, future):
        """After a new page is fetched, updates the internal state
        of the iterator and returns whether or not there are some
        rows in the fetched page.

        Args:
            future (hazelcast.future.Future):

        Returns:
            hazelcast.future.Future:
        """
        page = future.result()
        self.on_next_page(page)
        return self._has_next()


class _BlockingIterator(_IteratorBase):
    """An iterator that blocks when the current page is exhausted
    and we need to fetch a new page from the server. Otherwise,
    it returns immediately with an object.

    This version is more performant than the Future-producing
    counterpart in a sense that, it does not box everything with
    a Future object.
    """

    def __iter__(self):
        return self

    def __next__(self):
        if not self._has_next():
            # No more rows are left.
            raise StopIteration

        row = self._get_current_row()
        self.position += 1
        return SqlRow(self.row_metadata, row, self.deserialize_fn)

    def _has_next(self):
        while self.position == self.row_count:
            # We exhausted the current page.

            if self.is_last:
                # No more rows left on the server.
                return False

            # Block while waiting for the next page.
            page = self.fetch_fn().result()

            # Update the internal state with the next page.
            self.on_next_page(page)

        # There are some rows left in the current page.
        return True


class SqlResult(object):
    """SQL query result.

    Depending on the statement type it represents a stream of
    rows or an update count.

    To iterate over the stream of rows, there are two possible options.

    The first, and the easiest one is to iterate over the rows
    in a blocking fashion. ::

        result = client.sql.execute("SELECT ...").result()
        for row in result:
            # Process the row.
            print(row)

    The second option is to use the non-blocking API with callbacks. ::

        future = client.sql.execute("SELECT ...")  # Future of SqlResult

        def on_response(sql_result_future):
            iterator = sql_result_future.result().iterator()

            def on_next_row(row_future):
                try:
                    row = row_future.result()
                    # Process the row.
                    print(row)

                    # Iterate over the next row.
                    next(iterator).add_done_callback(on_next_row)
                except StopIteration:
                    # Exhausted the iterator. No more rows are left.
                    pass

            next(iterator).add_done_callback(on_next_row)

        future.add_done_callback(on_response)

    When in doubt, use the blocking API shown in the first code sample.

    Note that, iterators can be requested at most once per SqlResult.

    One can call :func:`close` method of a result object to
    release the resources associated with the result on the server side.
    It might also be used to cancel query execution on the server side
    if it is still active.

    When the blocking API is used, one might also use ``with``
    statement to automatically close the query even if an exception
    is thrown in the iteration. ::

        with client.sql.execute("SELECT ...").result() as result:
            for row in result:
                # Process the row.
                print(row)


    To get the number of rows updated by the query, use the
    :func:`update_count`. ::

        update_count = client.sql.execute("UPDATE ...").result().update_count()

    One does not have to call :func:`close` in this case, because the result
    will already be closed in the server-side.
    """

    def __init__(self, sql_service, connection, query_id, cursor_buffer_size, execute_response):
        self._sql_service = sql_service
        """_InternalSqlService: Reference to the SQL service."""

        self._connection = connection
        """hazelcast.connection.Connection: Reference to the connection
        that the execute request is made to."""

        self._query_id = query_id
        """_SqlQueryId: Unique id of the SQL query."""

        self._cursor_buffer_size = cursor_buffer_size
        """int: Size of the cursor buffer measured in the number of rows."""

        self._lock = RLock()
        """RLock: Protects the shared access to instance variables below."""

        self._execute_response = execute_response
        """_ExecuteResponse: Response for the first execute request."""

        self._iterator_requested = False
        """bool: Flag that shows whether an iterator is already requested."""

        self._closed = self._is_closed(execute_response)
        """bool: Flag that shows whether the query execution is still active
        on the server side. When ``True``, there is no need to send the "close" 
        request to the server."""

        self._fetch_future = None
        """Future: Will be set, if there are more pages to fetch on the server
        side. It should be set to ``None`` once the fetch is completed."""

    def iterator(self):
        """Returns the iterator over the result rows.

        The iterator may be requested only once.

        Raises:
            ValueError: If the result only contains an update count, or the
                iterator is already requested.

        Returns:
            Iterator[Future[SqlRow]]: Iterator that produces Future of
            :class:`SqlRow` s. See the class documentation for the correct
            way to use this.
        """
        return self._get_iterator(False)

    def is_row_set(self):
        """Returns whether this result has rows to iterate.

        Returns:
            bool:
        """
        # By design, if the row_metadata (or row_page) is None,
        # we only got the update count.
        return self._execute_response.row_metadata is not None

    def update_count(self):
        """Returns the number of rows updated by the statement or ``-1`` if this
        result is a row set. In case the result doesn't contain rows but the
        update count isn't applicable or known, ``0`` is returned.

        Returns:
            int:
        """
        # This will be set to -1, when we got row set on the client side.
        # See _on_execute_response.
        return self._execute_response.update_count

    def get_row_metadata(self):
        """Gets the row metadata.

        Raises:
            ValueError: If the result only contains an update count.

        Returns:
            SqlRowMetadata:
        """

        response = self._execute_response
        if not response.row_metadata:
            raise ValueError("This result contains only update count")

        return response.row_metadata

    def close(self):
        """Release the resources associated with the query result.

        The query engine delivers the rows asynchronously. The query may
        become inactive even before all rows are consumed. The invocation
        of this command will cancel the execution of the query on all members
        if the query is still active. Otherwise it is no-op. For a result
        with an update count it is always no-op.

        The returned Future results with:

        - :class:`HazelcastSqlError`: In case there is an error closing the
          result.

        Returns:
            Future[None]:
        """

        with self._lock:
            if self._closed:
                # Do nothing if the result is already closed.
                return ImmediateFuture(None)

            error = HazelcastSqlError(
                self._sql_service.get_client_id(),
                _SqlErrorCode.CANCELLED_BY_USER,
                "Query was cancelled by the user",
                None,
            )

            if not self._fetch_future:
                # Make sure that all subsequent fetches will fail.
                self._fetch_future = Future()

            self._on_fetch_error(error)

            def wrap_error_on_failure(f):
                # If the close request is failed somehow,
                # wrap it in a HazelcastSqlError.
                try:
                    return f.result()
                except Exception as e:
                    raise self._sql_service.re_raise(e, self._connection)

            self._closed = True

            # Send the close request
            return self._sql_service.close(self._connection, self._query_id).continue_with(
                wrap_error_on_failure
            )

    def __iter__(self):
        # Get the blocking iterator
        return self._get_iterator(True)

    def _get_iterator(self, should_get_blocking):
        """Gets the iterator after the execute request finishes.

        Args:
            should_get_blocking (bool): Whether to get a blocking iterator.

        Returns:
            Iterator:
        """
        response = self._execute_response
        if not response.row_metadata:
            # Can't get an iterator when we only have update count
            raise ValueError("This result contains only update count")

        with self._lock:
            if self._iterator_requested:
                # Can't get an iterator when we already get one
                raise ValueError("Iterator can be requested only once")

            self._iterator_requested = True

        if should_get_blocking:
            iterator = _BlockingIterator(
                response.row_metadata,
                self._fetch_next_page,
                self._sql_service.deserialize_object,
            )
        else:
            iterator = _FutureProducingIterator(
                response.row_metadata,
                self._fetch_next_page,
                self._sql_service.deserialize_object,
            )

        # Pass the first page information to the iterator
        iterator.on_next_page(response.row_page)
        return iterator

    def _fetch_next_page(self):
        """Fetches the next page, if there is no fetch request
        in-flight.

        Returns:
            Future[_SqlPage]:
        """
        with self._lock:
            if self._fetch_future:
                # A fetch request is already in-flight, return it.
                return self._fetch_future

            future = Future()
            self._fetch_future = future

            self._sql_service.fetch(
                self._connection, self._query_id, self._cursor_buffer_size
            ).add_done_callback(self._handle_fetch_response)

            # Need to return future, not self._fetch_future, because through
            # some unlucky timing, we might call _handle_fetch_response
            # before returning, which could set self._fetch_future to
            # None.
            return future

    def _handle_fetch_response(self, future):
        """Handles the result of the fetch request, by either:

        - setting it to exception, so that the future calls to
          fetch fails immediately.
        - setting it to next page, and setting self._fetch_future
          to None so that the next fetch request might actually
          try to fetch something from the server.

        Args:
            future (Future): The response from the server for
            the fetch request.
        """
        try:
            response = future.result()

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
                self._on_fetch_error(sql_error)
                return

            # The result contains the next page, as expected.
            self._on_fetch_response(response["row_page"])
        except Exception as e:
            # Something went bad, we couldn't get response from
            # the server, invocation failed.
            self._on_fetch_error(self._sql_service.re_raise(e, self._connection))

    def _on_fetch_error(self, error):
        """Sets the fetch future with exception, but not resetting it
        so that the next fetch request fails immediately.

        Args:
            error (Exception): The error.
        """
        with self._lock:
            self._fetch_future.set_exception(error)

    def _on_fetch_response(self, page):
        """Sets the fetch future with the next page,
        resets it, and if this is the last page,
        marks the result as closed.

        Args:
            page (_SqlPage): The next page.
        """
        with self._lock:
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
        """Returns whether the result is already
        closed or not.

        Result might be closed if the first response

        - contains the last page of the rowset (single page rowset)
        - contains just the update count

        Args:
            execute_response (_ExecuteResponse): The first response

        Returns:
            bool: ``True`` if the result is already closed, ``False``
            otherwise.
        """
        return (
            execute_response.row_metadata is None  # Just an update count
            or execute_response.row_page.is_last  # Single page result
        )

    def __enter__(self):
        # The response for the execute request is already
        # received. There is nothing more to do.
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Ignoring the possible exception details
        # since we close the query regardless of that.
        self.close().result()


class _InternalSqlService(object):
    """Internal SQL service that offers more public API
    than the one exposed to the user.
    """

    def __init__(self, connection_manager, serialization_service, invocation_service):
        self._connection_manager = connection_manager
        self._serialization_service = serialization_service
        self._invocation_service = invocation_service

    def execute(self, sql, params, kwargs):
        """Constructs a statement and executes it.

        Args:
            sql (str): SQL string.
            params (tuple): Query parameters.
            kwargs (dict): Arguments to customize the query.

        Returns:
            hazelcast.future.Future[SqlResult]: The execution result.
        """
        statement = _SqlStatement(sql, params, **kwargs)

        connection = None
        try:
            connection = self._get_query_connection()
            # Create a new, unique query id.
            query_id = _SqlQueryId.from_uuid(connection.remote_uuid)

            # Serialize the passed parameters.
            serialized_params = [
                self._serialization_service.to_data(param) for param in statement.parameters
            ]

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
                request, connection=connection, response_handler=sql_execute_codec.decode_response
            )
            self._invocation_service.invoke(invocation)
            return invocation.future.continue_with(
                lambda future: SqlResult(
                    self,
                    connection,
                    query_id,
                    statement.cursor_buffer_size,
                    self._handle_execute_response(future, connection),
                )
            )
        except Exception as e:
            return ImmediateExceptionFuture(self.re_raise(e, connection))

    def deserialize_object(self, obj):
        try:
            return self._serialization_service.to_object(obj)
        except Exception as e:
            raise HazelcastSqlError(
                self.get_client_id(),
                _SqlErrorCode.GENERIC,
                "Failed to deserialize query result value: %s" % try_to_get_error_message(e),
                e,
            )

    def fetch(self, connection, query_id, cursor_buffer_size):
        """Fetches the next page of the query execution.

        Args:
            connection (hazelcast.connection.Connection): Connection
                that the first execute request, hence the fetch request
                must route to.
            query_id (_SqlQueryId): Unique id of the query.
            cursor_buffer_size (int): Size of cursor buffer. Same as
                the one used in the first execute request.

        Returns:
            Future: Decoded fetch response.
        """
        request = sql_fetch_codec.encode_request(query_id, cursor_buffer_size)
        invocation = Invocation(
            request, connection=connection, response_handler=sql_fetch_codec.decode_response
        )
        self._invocation_service.invoke(invocation)
        return invocation.future

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

    def close(self, connection, query_id):
        """Closes the remote query cursor.

        Args:
            connection (hazelcast.connection.Connection): Connection
                that the first execute request, hence the close request
                must route to.
            query_id (_SqlQueryId): The query id to close.

        Returns:
            Future:
        """
        request = sql_close_codec.encode_request(query_id)
        invocation = Invocation(request, connection=connection)
        self._invocation_service.invoke(invocation)
        return invocation.future

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

    def _handle_execute_response(self, future, connection):
        """Handles the result of the execute request.

        Args:
            future (Future): The execute request's future.

        Returns:
            _ExecuteResponse: The response for the first execute request.
            It can be either a rowset or an update count response.
        """
        try:
            response = future.result()
        except Exception as e:
            # Something went bad, we couldn't get response from
            # the server, invocation failed.
            raise self.re_raise(e, connection)

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


class SqlExpectedResultType(object):
    """The expected statement result type."""

    ANY = 0
    """
    The statement may produce either rows or an update count.
    """

    ROWS = 1
    """
    The statement must produce rows. An exception is thrown is the statement produces an update count.
    """

    UPDATE_COUNT = 2
    """
    The statement must produce an update count. An exception is thrown is the statement produces rows.
    """


_TIMEOUT_NOT_SET = -1
_DEFAULT_CURSOR_BUFFER_SIZE = 4096


class _SqlStatement(object):
    """Definition of an SQL statement."""

    def __init__(
        self,
        sql,
        parameters,
        timeout=_TIMEOUT_NOT_SET,
        cursor_buffer_size=_DEFAULT_CURSOR_BUFFER_SIZE,
        schema=None,
        expected_result_type=SqlExpectedResultType.ANY,
    ):
        self.sql = sql
        self.parameters = parameters
        self.timeout = timeout
        self.cursor_buffer_size = cursor_buffer_size
        self.schema = schema
        self.expected_result_type = expected_result_type

    @property
    def sql(self):
        return self._sql

    @sql.setter
    def sql(self, sql):
        check_true(isinstance(sql, str), "SQL must be a string")

        if not sql.strip():
            raise ValueError("SQL cannot be empty")

        self._sql = sql

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, schema):
        check_true(
            isinstance(schema, str) or schema is None,
            "Schema must be a string or None",
        )
        self._schema = schema

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, timeout):
        check_is_number(timeout, "Timeout must be an integer or float")
        if timeout < 0 and timeout != _TIMEOUT_NOT_SET:
            raise ValueError("Timeout must be non-negative or -1, not %s" % timeout)

        self._timeout = timeout

    @property
    def cursor_buffer_size(self):
        return self._cursor_buffer_size

    @cursor_buffer_size.setter
    def cursor_buffer_size(self, cursor_buffer_size):
        check_is_int(cursor_buffer_size, "Cursor buffer size must an integer")
        if cursor_buffer_size <= 0:
            raise ValueError("Cursor buffer size must be positive, not %s" % cursor_buffer_size)
        self._cursor_buffer_size = cursor_buffer_size

    @property
    def expected_result_type(self):
        return self._expected_result_type

    @expected_result_type.setter
    def expected_result_type(self, expected_result_type):
        self._expected_result_type = try_to_get_enum_value(
            expected_result_type, SqlExpectedResultType
        )

    def __repr__(self):
        return (
            "SqlStatement(schema=%s, sql=%s, parameters=%s, timeout=%s,"
            " cursor_buffer_size=%s, expected_result_type=%s)"
            % (
                self.schema,
                self.sql,
                self.parameters,
                self.timeout,
                self.cursor_buffer_size,
                self._expected_result_type,
            )
        )


# These are imported at the bottom of the page to get rid of the
# cyclic import errors.
from hazelcast.protocol.codec import sql_execute_codec, sql_fetch_codec, sql_close_codec
