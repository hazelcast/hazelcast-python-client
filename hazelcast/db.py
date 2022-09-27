
from collections import namedtuple
from datetime import date, datetime, time
from time import localtime
from typing import Any, Dict, Callable, Iterator, List, Optional, Sequence, Union
import enum
import itertools
import threading

from hazelcast import HazelcastClient
from hazelcast.sql import HazelcastSqlError, SqlColumnType, SqlResult, SqlRow, SqlRowMetadata, SqlExpectedResultType
import hazelcast.errors as _err

apilevel = "2.0"
# Threads may share the module and connections.
threadsafety = 2
paramstyle = "qmark"

ResultColumn = namedtuple("ResultColumn", [
    'name', 'type', 'display_size', 'internal_size',
    'precision', 'scale', 'null_ok',
])

Date = date
Time = time
Timestamp = datetime
Binary = bytes
STRING = str
BINARY = bytes
NUMBER = float
DATETIME = datetime


class Type(enum.Enum):
    STRING = 1
    NUMBER = 2
    BOOLEAN = 3
    DATETIME = 4


def DateFromTicks(ticks):
    return date(*localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return time(*localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return datetime(*localtime(ticks)[:6])


class RowResult:

    def __init__(self, result):
        self._result = result

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class Cursor:

    def __init__(self, conn: 'Connection'):
        self.arraysize = 0
        self._conn = conn
        self._res: Union[SqlResult, None] = None
        self._description: Union[List[ResultColumn], None] = None
        self._iter: Optional[Iterator[SqlRow]] = None
        self._rownumber = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self) -> Optional[Iterator[SqlRow]]:
        return self._iter

    @property
    def connection(self):
        return self._conn

    @property
    def description(self) -> Union[List[ResultColumn], None]:
        return self._description

    @property
    def rowcount(self) -> int:
        return -1

    @property
    def rownumber(self):
        return self._rownumber

    def close(self):
        if self._res:
            self._res.close()
            self._res = None

    def execute(self, operation: str, *args) -> None:
        self._rownumber = None
        self._iter = None
        self._res = None
        kwargs = {}
        if self.arraysize > 0:
            kwargs["cursor_buffer_size"] = self.arraysize
        res = self._conn._client.sql.execute(operation, *args, **kwargs).result()
        if res.is_row_set():
            self._rownumber = 0
            self._res = res
            self._description = self._make_description(res.get_row_metadata())
            self._iter = res.__iter__()

    def executemany(self, operation: str, seq_of_params: Sequence[Any]) -> None:
        self._rownumber = None
        self._iter = None
        self._res = None
        futures = []
        svc = self._conn._client.sql
        for params in seq_of_params:
            futures.append(svc.execute(operation, *params,
               expected_result_type=SqlExpectedResultType.UPDATE_COUNT))
        for fut in futures:
            fut.result()

    def fetchone(self) -> Optional[SqlRow]:
        if self._iter is None:
            return None
        row = next(self._iter)
        self._rownumber += 1
        return row

    def fetchmany(self, size=None) -> List[SqlRow]:
        if self._iter is None:
            return []
        rows = list(itertools.islice(self._iter, size))
        self._rownumber += len(rows)
        return rows

    def fetchall(self) -> List[SqlRow]:
        if self._rownumber is None:
            self._rownumber = 0
        if self._iter is None:
            return []
        rows = list(self._iter)
        self._rownumber += len(rows)
        return rows

    def next(self) -> Optional[SqlRow]:
        if self._iter is None:
            return
        return next(self._iter)

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, column=None):
        pass

    @classmethod
    def _make_description(cls, metadata: SqlRowMetadata) -> List[ResultColumn]:
        r = []
        for col in metadata.columns:
            r.append(ResultColumn(
                name=col.name, type=map_type(col.type),
                display_size=None, internal_size=None,
                precision=None, scale=None, null_ok=col.nullable,
            ))
        return r


class Connection:

    def __init__(self, config: Dict[str, Any]):
        self._mu = threading.RLock()
        self._client = HazelcastClient(**config)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self._client:
            with self._mu:
                if self._client:
                    self._client.shutdown()
                    self._client = None

    def commit(self):
        # transactions are not supported
        pass

    def cursor(self) -> Cursor:
        with self._mu:
            if self._client is not None:
                return Cursor(self)
        raise ProgrammingError("connection is already closed")

    @property
    def Error(self):
        return Error

    @property
    def Warning(self):
        return Warning

    @property
    def InterfaceError(self):
        return InterfaceError

    @property
    def DatabaseError(self):
        return DatabaseError

    @property
    def InternalError(self):
        return InternalError

    @property
    def OperationalError(self):
        return OperationalError

    @property
    def ProgrammingError(self):
        return ProgrammingError

    @property
    def IntegrityError(self):
        return IntegrityError

    @property
    def DataError(self):
        return DataError

    @property
    def NotSupportedError(self):
        return NotSupportedError


def connect(config=None, *, dsn="", user: str=None, password: str=None, host="localhost", port: int=None) -> Connection:
    if config is not None:
        return Connection(config)
    if port:
        host = f"{host}:{port}"
    config = {
        "cluster_members": [host],
        "creds_username": user,
        "creds_password": password,
    }
    return Connection(config)


class Error(Exception):
    pass


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


def wrap_error(f: Callable) -> Any:
    try:
        return f()
    except HazelcastSqlError as e:
        msg = f"{e.args}"
        raise DatabaseError from e
    except Exception as e:
        raise Error from e


def map_type(code: int) -> Type:
    type = _type_map.get(code)
    if type is None:
        raise NotSupportedError(f"unknown type code: {code}")
    return type


_type_map = {
    SqlColumnType.VARCHAR: Type.STRING,
    SqlColumnType.BOOLEAN: Type.BOOLEAN,
    SqlColumnType.TINYINT: Type.NUMBER,
    SqlColumnType.SMALLINT: Type.NUMBER,
    SqlColumnType.INTEGER: Type.STRING,
    SqlColumnType.BIGINT: Type.NUMBER,
    SqlColumnType.DECIMAL: Type.NUMBER,
    SqlColumnType.REAL: Type.NUMBER,
    SqlColumnType.DOUBLE: Type.NUMBER,
    SqlColumnType.DATE: Type.DATETIME,
    SqlColumnType.TIME: Type.DATETIME,
    SqlColumnType.TIMESTAMP: Type.DATETIME,
    SqlColumnType.TIMESTAMP_WITH_TIME_ZONE: Type.DATETIME,
    # SqlColumnType.OBJECT:
    # SqlColumnType.NULL:
    # SqlColumnType.JSON:
}


_error_map = {}

for e in [
    _err._ARRAY_INDEX_OUT_OF_BOUNDS, _err._ARRAY_STORE, _err._CLASS_CAST,
    _err._CLASS_NOT_FOUND, _err._ILLEGAL_ACCESS_EXCEPTION, _err._ILLEGAL_ACCESS_ERROR,
    _err._ILLEGAL_MONITOR_STATE, _err._ILLEGAL_STATE, _err._ILLEGAL_THREAD_STATE,
    _err._INDEX_OUT_OF_BOUNDS, _err._INTERRUPTED, _err._INVALID_ADDRESS,
    _err._NEGATIVE_ARRAY_SIZE, _err._NULL_POINTER, _err._REACHED_MAX_SIZE,
    _err._RUNTIME, _err._XA, _err._ASSERTION_ERROR, _err._SERVICE_NOT_FOUND,
    _err._LOCAL_MEMBER_RESET, _err._INDETERMINATE_OPERATION_STATE,
]:
    _error_map[e] = InternalError

for e in [
    _err._AUTHENTICATION, _err._CACHE_NOT_EXISTS, _err._CONFIG_MISMATCH,
    _err._DISTRIBUTED_OBJECT_DESTROYED, _err._ILLEGAL_ARGUMENT, _err._INVALID_CONFIGURATION,
    _err._NO_SUCH_ELEMENT, _err._QUERY, _err._QUERY_RESULT_SIZE_EXCEEDED,
    _err._SPLIT_BRAIN_PROTECTION, _err._RESPONSE_ALREADY_SENT, _err._SECURITY,
    _err._STALE_SEQUENCE,_err._ACCESS_CONTROL, _err._LOGIN,

]:
    _error_map[e] = ProgrammingError

for e in [
    _err._CALLER_NOT_MEMBER, _err._CANCELLATION, _err._CONCURRENT_MODIFICATION,
    _err._EOF, _err._EXECUTION, _err._HAZELCAST_OVERLOAD, _err._IO,
    _err._MEMBER_LEFT, _err._OPERATION_TIMEOUT, _err._PARTITION_MIGRATING,
    _err._RETRYABLE_HAZELCAST, _err._RETRYABLE_IO, _err._TARGET_DISCONNECTED,
    _err._TARGET_NOT_MEMBER, _err._TIMEOUT, _err._NO_DATA_MEMBER,
    _err._WAN_REPLICATION_QUEUE_FULL, _err._OUT_OF_MEMORY_ERROR, _err._STACK_OVERFLOW_ERROR,
    _err._NATIVE_OUT_OF_MEMORY_ERROR, _err._MUTATION_DISALLOWED_EXCEPTION, _err._CONSISTENCY_LOST_EXCEPTION,
    _err._WAIT_KEY_CANCELLED_EXCEPTION, _err._LOCK_OWNERSHIP_LOST_EXCEPTION, _err._CP_GROUP_DESTROYED_EXCEPTION,
    _err._STALE_APPEND_REQUEST_EXCEPTION, _err._NOT_LEADER_EXCEPTION,
]:
    _error_map[e] = OperationalError

for e in [
    _err._HAZELCAST, _err._REJECTED_EXECUTION, _err._TOPIC_OVERLOAD,
    _err._TRANSACTION, _err._TRANSACTION_NOT_ACTIVE, _err._TRANSACTION_TIMED_OUT,
    _err._REPLICATED_MAP_CANT_BE_CREATED, _err._STALE_TASK_ID, _err._DUPLICATE_TASK,
    _err._STALE_TASK, _err._LOCK_ACQUIRE_LIMIT_REACHED_EXCEPTION, _err._CANNOT_REPLICATE_EXCEPTION,
    _err._LEADER_DEMOTED_EXCEPTION,
]:
    _error_map[e] = DatabaseError

for e in [
    _err._HAZELCAST_INSTANCE_NOT_ACTIVE, _err._SOCKET, _err._WRONG_TARGET,
    _err._MAX_MESSAGE_SIZE_EXCEEDED, _err._TARGET_NOT_REPLICA_EXCEPTION, _err._VERSION_MISMATCH_EXCEPTION,
]:
    _error_map[e] = InterfaceError

for e in [
    _err._HAZELCAST_SERIALIZATION, _err._NOT_SERIALIZABLE, _err._URI_SYNTAX,
    _err._UTF_DATA_FORMAT,
]:
    _error_map[e] = DataError

for e in [
    _err._UNSUPPORTED_OPERATION, _err._UNSUPPORTED_CALLBACK, _err._NO_SUCH_METHOD_ERROR,
    _err._NO_SUCH_METHOD_EXCEPTION, _err._NO_SUCH_FIELD_ERROR, _err._NO_SUCH_FIELD_EXCEPTION,
    _err._NO_CLASS_DEF_FOUND_ERROR,
]:
    _error_map[e] = NotSupportedError
