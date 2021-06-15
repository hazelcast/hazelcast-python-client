import logging
import uuid
from threading import RLock

from hazelcast.errors import HazelcastError
from hazelcast.future import Future, ImmediateFuture
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
from hazelcast import six

_logger = logging.getLogger(__name__)


class SqlService(object):
    """A service to execute SQL statements.

    The service allows you to query data stored in a
    :class:`Map <hazelcast.proxy.map.Map>`.

    Warnings:

        The service is in beta state. Behavior and API might change
        in future releases.

    **Querying an IMap**

    Every Map instance is exposed as a table with the same name in the
    ``partitioned`` schema. The ``partitioned`` schema is included into
    a default search path, therefore a Map could be referenced in an
    SQL statement with or without the schema name.

    **Column resolution**

    Every table backed by a Map has a set of columns that are resolved
    automatically. Column resolution uses Map entries located on the
    member that initiates the query. The engine extracts columns from a
    key and a value and then merges them into a single column set.
    In case the key and the value have columns with the same name, the
    key takes precedence.

    Columns are extracted from objects as follows (which happens on the
    server-side):

    - For non-Portable objects, public getters and fields are used to
      populate the column list. For getters, the first letter is converted
      to lower case. A getter takes precedence over a field in case of naming
      conflict.
    - For :class:`Portable <hazelcast.serialization.api.Portable>` objects,
      field names used in the
      :func:`write_portable() <hazelcast.serialization.api.Portable.write_portable>`
      method are used to populate the column list.

    The whole key and value objects could be accessed through special fields
    ``__key`` and ``this``, respectively. If key (value) object has fields,
    then the whole key (value) field is exposed as a normal field. Otherwise the
    field is hidden. Hidden fields can be accessed directly, but are not returned
    by ``SELECT * FROM ...`` queries.

    Consider the following key/value model: ::

        class PersonKey(Portable):
            def __init__(self, person_id=None, department_id=None):
                self.person_id = person_id
                self.department_id = department_id

            def write_portable(self, writer):
                writer.write_long("person_id", self.person_id)
                writer.write_long("department_id", self.department_id)

            ...

        class Person(Portable):
            def __init__(self, name=None):
                self.name = name

            def write_portable(self, writer):
                writer.write_string("name", self.name)

            ...

    This model will be resolved to the following table columns:

    - person_id ``BIGINT``
    - department_id ``BIGINT``
    - name ``VARCHAR``
    - __key ``OBJECT`` (hidden)
    - this ``OBJECT`` (hidden)

    **Consistency**

    Results returned from Map query are weakly consistent:

    - If an entry was not updated during iteration, it is guaranteed to be
      returned exactly once
    - If an entry was modified during iteration, it might be returned zero,
      one or several times

    **Usage**

    When a query is executed, an :class:`SqlResult` is returned. You may get
    row iterator from the result. The result must be closed at the end. The
    iterator will close the result automatically when it is exhausted given
    that no error is raised during the iteration. The code snippet below
    demonstrates a typical usage pattern: ::

        client = hazelcast.HazelcastClient()

        result = client.sql.execute("SELECT * FROM person")

        for row in result:
            print(row.get_object("person_id"))
            print(row.get_object("name"))
            ...


    See the documentation of the :class:`SqlResult` for more information about
    different iteration methods.

    Notes:

        When an SQL statement is submitted to a member, it is parsed and
        optimized by the ``hazelcast-sql`` module. The ``hazelcast-sql`` must
        be in the classpath, otherwise an exception will be thrown. If you're
        using the ``hazelcast-all`` or ``hazelcast-enterprise-all`` packages, the
        ``hazelcast-sql`` module is included in them by default. If not, i.e., you
        are using ``hazelcast`` or ``hazelcast-enterprise``, then you need to have
        ``hazelcast-sql`` in the classpath. If you are using the Docker image,
        the SQL module is included by default.

    """

    def __init__(self, internal_sql_service):
        self._service = internal_sql_service

    def execute(self, sql, *params):
        """Convenient method to execute a distributed query with the given
        parameters.

        Converts passed SQL string and parameters into an :class:`SqlStatement`
        object and invokes :func:`execute_statement`.

        Args:
            sql (str): SQL string.
            *params: Query parameters that will be passed to
                :func:`SqlStatement.add_parameter`.

        Returns:
            SqlResult: The execution result.

        Raises:
            HazelcastSqlError: In case of execution error.
            AssertionError: If the SQL parameter is not a string.
            ValueError: If the SQL parameter is an empty string.
        """
        return self._service.execute(sql, *params)

    def execute_statement(self, statement):
        """Executes an SQL statement.

        Args:
            statement (SqlStatement): Statement to be executed

        Returns:
           SqlResult: The execution result.

        Raises:
            HazelcastSqlError: In case of execution error.
        """
        return self._service.execute_statement(statement)


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

    __slots__ = ("code", "message", "originating_member_uuid")

    def __init__(self, code, message, originating_member_uuid):
        self.code = code
        """_SqlErrorCode: The error code."""

        self.message = message
        """str: The error message."""

        self.originating_member_uuid = originating_member_uuid
        """uuid.UUID: UUID of the member that caused or initiated an error condition."""


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
    Represented by ``int`` (for Python 3) or ``long`` (for Python 2).
    """

    DECIMAL = 6
    """
    Represented by ``str``.
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
    Represented by ``str`` with the ``YYYY-MM-DD`` format.
    """

    TIME = 10
    """
    Represented by ``str`` with the ``HH:MM:SS[.ffffff]`` format.
    """

    TIMESTAMP = 11
    """
    Represented by ``str`` with the ``YYYY-MM-DDTHH:MM:SS[.ffffff]`` format.
    """

    TIMESTAMP_WITH_TIME_ZONE = 12
    """
    Represented by ``str`` with the 
    ``YYYY-MM-DDTHH:MM:SS[.ffffff](+|-)HH:MM[:SS]`` format.
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

    MAP_DESTROYED = 1006
    """
    An error caused by a concurrent destroy of a map.
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

    DATA_EXCEPTION = 2000
    """
    An error with data conversion or transformation.
    """


class HazelcastSqlError(HazelcastError):
    """Represents an error occurred during the SQL query execution."""

    def __init__(self, originating_member_uuid, code, message, cause):
        super(HazelcastSqlError, self).__init__(message, cause)
        self._originating_member_uuid = originating_member_uuid

        # TODO: This is private API, might be good to make it public or
        #  remove this information altogether.
        self._code = code

    @property
    def originating_member_uuid(self):
        """uuid.UUID: UUID of the member that caused or initiated an error condition."""
        return self._originating_member_uuid


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
        check_true(isinstance(column_name, six.string_types), "Column name must be a string")
        return self._name_to_index.get(column_name, SqlRowMetadata.COLUMN_NOT_FOUND)

    def __repr__(self):
        return "[%s]" % ", ".join(
            "%s %s" % (column.name, get_attr_name(SqlColumnType, column.type))
            for column in self._columns
        )


class SqlRow(object):
    """One of the rows of an SQL query result."""

    __slots__ = ("_row_metadata", "_row")

    def __init__(self, row_metadata, row):
        self._row_metadata = row_metadata
        self._row = row

    def get_object(self, column_name):
        """Gets the value in the column indicated by the column name.

        Column name should be one of those defined in :class:`SqlRowMetadata`,
        case-sensitive. You may also use :func:`SqlRowMetadata.find_column` to
        test for column existence.

        The type of the returned value depends on the SQL type of the column.
        No implicit conversions are performed on the value.

        Args:
            column_name (str):

        Returns:
            Value of the column.

        Raises:
            ValueError: If a column with the given name does not exist.
            AssertionError: If the column name is not a string.

        See Also:
            :attr:`metadata`

            :func:`SqlRowMetadata.find_column`

            :attr:`SqlColumnMetadata.type`

            :attr:`SqlColumnMetadata.name`
        """
        index = self._row_metadata.find_column(column_name)
        if index == SqlRowMetadata.COLUMN_NOT_FOUND:
            raise ValueError("Column '%s' doesn't exist" % column_name)
        return self._row[index]

    def get_object_with_index(self, column_index):
        """Gets the value of the column by index.

        The class of the returned value depends on the SQL type of the column.
        No implicit conversions are performed on the value.

        Args:
            column_index (int): Zero-based column index.

        Returns:
            Value of the column.

        Raises:
            IndexError: If the column index is out of bounds.
            AssertionError: If the column index is not an integer.

        See Also:
            :attr:`metadata`

            :attr:`SqlColumnMetadata.type`
        """
        check_is_int(column_index, "Column index must be an integer")
        return self._row[column_index]

    @property
    def metadata(self):
        """SqlRowMetadata: The row metadata."""
        return self._row_metadata

    def __repr__(self):
        return "[%s]" % ", ".join(
            "%s %s=%s"
            % (
                self._row_metadata.get_column(i).name,
                get_attr_name(SqlColumnType, self._row_metadata.get_column(i).type),
                self._row[i],
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

        # The column might contain user objects so we have to deserialize it.
        # Deserialization is no-op if the value is not Data.
        return [
            self.deserialize_fn(self.page.get_value(i, self.position))
            for i in range(self.page.column_count)
        ]


class _FutureProducingIterator(_IteratorBase):
    """An iterator that produces infinite stream of Futures. It is the
    responsibility of the user to either call them in blocking fashion,
    or call ``next`` only if the current call to next did not raise
    ``StopIteration`` error (possibly with callback-based code).
    """

    def __iter__(self):
        return self

    def next(self):
        # Defined for backward-compatibility with Python 2.
        return self.__next__()

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
        return SqlRow(self.row_metadata, row)

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

    def next(self):
        # Defined for backward-compatibility with Python 2.
        return self.__next__()

    def __next__(self):
        if not self._has_next():
            # No more rows are left.
            raise StopIteration

        row = self._get_current_row()
        self.position += 1
        return SqlRow(self.row_metadata, row)

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

        result = client.sql.execute("SELECT ...")
        for row in result:
            # Process the row.
            print(row)

    The second option is to use the non-blocking API with callbacks. ::

        result = client.sql.execute("SELECT ...")
        it = result.iterator()  # Future of iterator

        def on_iterator_response(iterator_future):
            iterator = iterator_future.result()

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

        it.add_done_callback(on_iterator_response)

    When in doubt, use the blocking API shown in the first code sample.

    Note that, iterators can be requested at most once per SqlResult.

    One can call :func:`close` method of a result object to
    release the resources associated with the result on the server side.
    It might also be used to cancel query execution on the server side
    if it is still active.

    When the blocking API is used, one might also use ``with``
    statement to automatically close the query even if an exception
    is thrown in the iteration. ::

        with client.sql.execute("SELECT ...") as result:
            for row in result:
                # Process the row.
                print(row)


    To get the number of rows updated by the query, use the
    :func:`update_count`. ::

        update_count = client.sql.execute("SELECT ...").update_count().result()

    One does not have to call :func:`close` in this case, because the result
    will already be closed in the server-side.
    """

    def __init__(self, sql_service, connection, query_id, cursor_buffer_size, execute_future):
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

        self._execute_response = Future()
        """Future: Will be resolved with :class:`_ExecuteResponse` once the
        execute request is resolved."""

        self._iterator_requested = False
        """bool: Flag that shows whether an iterator is already requested."""

        self._closed = False
        """bool: Flag that shows whether the query execution is still active
        on the server side. When ``True``, there is no need to send the "close" 
        request to the server."""

        self._fetch_future = None
        """Future: Will be set, if there are more pages to fetch on the server
        side. It should be set to ``None`` once the fetch is completed."""

        execute_future.add_done_callback(self._handle_execute_response)

    def iterator(self):
        """Returns the iterator over the result rows.

        The iterator may be requested only once.

        The returned Future results with:

        - :class:`HazelcastSqlError`: In case of an SQL execution error.
        - **ValueError**: If the result only contains an update count, or the
          iterator is already requested.

        Returns:
            Future[Iterator[Future[SqlRow]]]: Iterator that produces Future
            of :class:`SqlRow` s. See the class documentation for the correct
            way to use this.
        """
        return self._get_iterator(False)

    def is_row_set(self):
        """Returns whether this result has rows to iterate.

        The returned Future results with:

        - :class:`HazelcastSqlError`: In case of an SQL execution error.

        Returns:
            Future[bool]:
        """

        def continuation(future):
            response = future.result()
            # By design, if the row_metadata (or row_page) is None,
            # we only got the update count.
            return response.row_metadata is not None

        return self._execute_response.continue_with(continuation)

    def update_count(self):
        """Returns the number of rows updated by the statement or ``-1`` if this
        result is a row set. In case the result doesn't contain rows but the
        update count isn't applicable or known, ``0`` is returned.

        The returned Future results with:

        - :class:`HazelcastSqlError`: In case of an SQL execution error.

        Returns:
            Future[int]:
        """

        def continuation(future):
            response = future.result()
            # This will be set to -1, when we got row set on the client side.
            # See _on_execute_response.
            return response.update_count

        return self._execute_response.continue_with(continuation)

    def get_row_metadata(self):
        """Gets the row metadata.

        The returned Future results with:

        - :class:`HazelcastSqlError`: In case of an SQL execution error.
        - **ValueError**: If the result only contains an update count.

        Returns:
            Future[SqlRowMetadata]:
        """

        def continuation(future):
            response = future.result()

            if not response.row_metadata:
                raise ValueError("This result contains only update count")

            return response.row_metadata

        return self._execute_response.continue_with(continuation)

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

            if not self._execute_response.done():
                # If the cancellation is initiated before the first response is
                # received, then throw cancellation errors on the dependent
                # methods (update count, row metadata, iterator).
                self._on_execute_error(error)

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
        # Get blocking iterator, and wait for the
        # first page.
        return self._get_iterator(True).result()

    def _get_iterator(self, should_get_blocking):
        """Gets the iterator after the execute request finishes.

        Args:
            should_get_blocking (bool): Whether to get a blocking iterator.

        Returns:
            Future[Iterator]:
        """

        def continuation(future):
            response = future.result()

            with self._lock:
                if not response.row_metadata:
                    # Can't get an iterator when we only have update count
                    raise ValueError("This result contains only update count")

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

        return self._execute_response.continue_with(continuation)

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

            response_error = self._handle_response_error(response["error"])
            if response_error:
                # There is a server side error sent to client.
                self._on_fetch_error(response_error)
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

    def _handle_execute_response(self, future):
        """Handles the result of the execute request, by either:

        - setting it to an exception so that the dependent methods
          (iterator, update_count etc.) fails immediately
        - setting it to an execute response

        Args:
            future (Future):
        """
        try:
            response = future.result()

            response_error = self._handle_response_error(response["error"])
            if response_error:
                # There is a server-side error sent to the client.
                self._on_execute_error(response_error)
                return

            row_metadata = response["row_metadata"]
            if row_metadata is not None:
                # The result contains some rows, not an update count.
                row_metadata = SqlRowMetadata(row_metadata)

            self._on_execute_response(row_metadata, response["row_page"], response["update_count"])
        except Exception as e:
            # Something went bad, we couldn't get response from
            # the server, invocation failed.
            self._on_execute_error(self._sql_service.re_raise(e, self._connection))

    @staticmethod
    def _handle_response_error(error):
        """If the error is not ``None``, return it as
        :class:`HazelcastSqlError` so that we can raise
        it to user.

        Args:
            error (_SqlError): The error or ``None``.

        Returns:
            HazelcastSqlError: If the error is not ``None``,
            ``None`` otherwise.
        """
        if error:
            return HazelcastSqlError(error.originating_member_uuid, error.code, error.message, None)
        return None

    def _on_execute_error(self, error):
        """Called when the first execute request is failed.

        Args:
            error (HazelcastSqlError): The wrapped error that can
            be raised to the user.
        """
        with self._lock:
            if self._closed:
                # User might be already cancelled it.
                return

            self._execute_response.set_exception(error)

    def _on_execute_response(self, row_metadata, row_page, update_count):
        """Called when the first execute request is succeeded.

        Args:
            row_metadata (SqlRowMetadata): The row metadata. Might be ``None``
                if the response only contains the update count.
            row_page (_SqlPage): The first page of the result. Might be
                ``None`` if the response only contains the update count.
            update_count (int): The update count.
        """
        with self._lock:
            if self._closed:
                # User might be already cancelled it.
                return

            if row_metadata:
                # Result contains the row set for the query.
                # Set the update count to -1.
                response = _ExecuteResponse(row_metadata, row_page, -1)

                if row_page.is_last:
                    # This is the last page, close the result.
                    self._closed = True

                self._execute_response.set_result(response)
            else:
                # Result only contains the update count.
                response = _ExecuteResponse(None, None, update_count)
                self._execute_response.set_result(response)

                # There is nothing more we can get from the server.
                self._closed = True

    def __enter__(self):
        # The execute request is already sent.
        # There is nothing more to do.
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

    def execute(self, sql, *params):
        """Constructs a statement and executes it.

        Args:
            sql (str): SQL string.
            *params: Query parameters.

        Returns:
            SqlResult: The execution result.
        """
        statement = SqlStatement(sql)

        for param in params:
            statement.add_parameter(param)

        return self.execute_statement(statement)

    def execute_statement(self, statement):
        """Executes the given statement.

        Args:
            statement (SqlStatement): The statement to execute.

        Returns:
            SqlResult: The execution result.
        """

        # Get a random Data member (non-lite member)
        connection = self._connection_manager.get_random_connection(True)
        if not connection:
            # Either the client is not connected to the cluster, or
            # there are no data members in the cluster.
            raise HazelcastSqlError(
                self.get_client_id(),
                _SqlErrorCode.CONNECTION_PROBLEM,
                "Client is not currently connected to the cluster.",
                None,
            )

        try:
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
            )

            invocation = Invocation(
                request, connection=connection, response_handler=sql_execute_codec.decode_response
            )

            result = SqlResult(
                self, connection, query_id, statement.cursor_buffer_size, invocation.future
            )

            self._invocation_service.invoke(invocation)

            return result
        except Exception as e:
            raise self.re_raise(e, connection)

    def deserialize_object(self, obj):
        return self._serialization_service.to_object(obj)

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
            connection (hazelcast.connection.Connection): Connection
                that the query requests are routed to. If it is not
                live, we will inform the user about the possible
                cluster topology change.

        Returns:
            HazelcastSqlError: The reraised error.
        """
        if not connection.live:
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


class SqlStatement(object):
    """Definition of an SQL statement.

    This object is mutable. Properties are read once before the execution
    is started. Changes to properties do not affect the behavior of already
    running statements.
    """

    TIMEOUT_NOT_SET = -1

    TIMEOUT_DISABLED = 0

    DEFAULT_TIMEOUT = TIMEOUT_NOT_SET

    DEFAULT_CURSOR_BUFFER_SIZE = 4096

    def __init__(self, sql):
        self.sql = sql
        self._parameters = []
        self._timeout = SqlStatement.DEFAULT_TIMEOUT
        self._cursor_buffer_size = SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE
        self._schema = None
        self._expected_result_type = SqlExpectedResultType.ANY

    @property
    def sql(self):
        """str: The SQL string to be executed.

        The setter raises:

        - **AssertionError**: If the SQL parameter is not a string.
        - **ValueError**: If the SQL parameter is an empty string.
        """
        return self._sql

    @sql.setter
    def sql(self, sql):
        check_true(isinstance(sql, six.string_types), "SQL must be a string")

        if not sql.strip():
            raise ValueError("SQL cannot be empty")

        self._sql = sql

    @property
    def schema(self):
        """str: The schema name. The engine will try to resolve the
        non-qualified object identifiers from the statement in the given
        schema. If not found, the default search path will be used, which
        looks for objects in the predefined schemas ``partitioned`` and
        ``public``.

        The schema name is case sensitive. For example, ``foo`` and ``Foo``
        are different schemas.

        The default value is ``None`` meaning only the default search path is
        used.

        The setter raises:

        - **AssertionError**: If the schema is not a string or ``None``.
        """
        return self._schema

    @schema.setter
    def schema(self, schema):
        check_true(
            isinstance(schema, six.string_types) or schema is None,
            "Schema must be a string or None",
        )
        self._schema = schema

    @property
    def parameters(self):
        """list: Sets the statement parameters.

        You may define parameter placeholders in the statement with the ``?``
        character. For every placeholder, a parameter value must be provided.

        When the setter is called, the content of the parameters list is copied.
        Subsequent changes to the original list don't change the statement parameters.

        The setter raises:

        - **AssertionError**: If the parameter is not a list.
        """
        return self._parameters

    @parameters.setter
    def parameters(self, parameters):
        check_true(isinstance(parameters, list), "Parameters must be a list")
        self._parameters = list(parameters)

    @property
    def timeout(self):
        """float or int: The execution timeout in seconds.

        If the timeout is reached for a running statement, it will be
        cancelled forcefully.

        Zero value means no timeout. :const:`TIMEOUT_NOT_SET` means that
        the value from the server-side config will be used. Other negative
        values are prohibited.

        Defaults to :const:`TIMEOUT_NOT_SET`.

        The setter raises:

        - **AssertionError**: If the timeout is not an integer or float.
        - **ValueError**: If the timeout is negative and not equal to
          :const:`TIMEOUT_NOT_SET`.
        """
        return self._timeout

    @timeout.setter
    def timeout(self, timeout):
        check_is_number(timeout, "Timeout must be an integer or float")
        if timeout < 0 and timeout != SqlStatement.TIMEOUT_NOT_SET:
            raise ValueError("Timeout must be non-negative or -1, not %s" % timeout)

        self._timeout = timeout

    @property
    def cursor_buffer_size(self):
        """int: The cursor buffer size (measured in the number of rows).

        When a statement is submitted for execution, a :class:`SqlResult`
        is returned as a result. When rows are ready to be consumed,
        they are put into an internal buffer of the cursor. This parameter
        defines the maximum number of rows in that buffer. When the threshold
        is reached, the backpressure mechanism will slow down the execution,
        possibly to a complete halt, to prevent out-of-memory.

        Only positive values are allowed.

        The default value is expected to work well for most workloads. A bigger
        buffer size may give you a slight performance boost for queries with
        large result sets at the cost of increased memory consumption.

        Defaults to :const:`DEFAULT_CURSOR_BUFFER_SIZE`.

        The setter raises:

        - **AssertionError**: If the cursor buffer size is not an integer.
        - **ValueError**: If the cursor buffer size is not positive.
        """
        return self._cursor_buffer_size

    @cursor_buffer_size.setter
    def cursor_buffer_size(self, cursor_buffer_size):
        check_is_int(cursor_buffer_size, "Cursor buffer size must an integer")
        if cursor_buffer_size <= 0:
            raise ValueError("Cursor buffer size must be positive, not %s" % cursor_buffer_size)
        self._cursor_buffer_size = cursor_buffer_size

    @property
    def expected_result_type(self):
        """SqlExpectedResultType: The expected result type.

        The setter raises:

        - **TypeError**: If the expected result type does not equal to one of
          the values or names of the members of the
          :class:`SqlExpectedResultType`.
        """
        return self._expected_result_type

    @expected_result_type.setter
    def expected_result_type(self, expected_result_type):
        self._expected_result_type = try_to_get_enum_value(
            expected_result_type, SqlExpectedResultType
        )

    def add_parameter(self, parameter):
        """Adds a single parameter to the end of the parameters list.

        Args:
            parameter: The parameter.

        See Also:
            :attr:`parameters`

            :func:`clear_parameters`
        """
        self._parameters.append(parameter)

    def clear_parameters(self):
        """Clears statement parameters."""
        self._parameters = []

    def copy(self):
        """Creates a copy of this instance.

        Returns:
            SqlStatement: The new copy.
        """
        copied = SqlStatement(self.sql)
        copied.parameters = list(self.parameters)
        copied.timeout = self.timeout
        copied.cursor_buffer_size = self.cursor_buffer_size
        copied.schema = self.schema
        copied.expected_result_type = self.expected_result_type
        return copied

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
