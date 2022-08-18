
from collections import namedtuple
from datetime import date, datetime, time
from time import localtime
from typing import Any, Iterator, List, Sequence, Union

from hazelcast import HazelcastClient
from hazelcast.sql import SqlResult, SqlRow, SqlRowMetadata

apilevel = "0.2"
# Threads may share the module and connections.
threadsafety = 2
paramstyle = "qmark"

ResultColumn = namedtuple("ResultColumn", [
    'name', 'type_code', 'display_size', 'internal_size',
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
        self._conn = conn
        self.arraysize = 1
        self._res: Union[SqlResult, None] = None
        self._description: Union[List[ResultColumn], None] = None

    @property
    def description(self) -> Union[List[ResultColumn], None]:
        return self._description

    @property
    def rowcount(self):
        return -1

    def close(self):
        if self._res:
            self._res.close()
            self._res = None

    def execute(self, operation: str, *args) -> Union[Iterator[SqlRow], None]:
        res = self._conn._client.sql.execute(operation, *args).result()
        if res.is_row_set():
            self._res = res
            self._description = self._make_description(res.get_row_metadata())
            return res.__iter__()

    def executemany(self, operation: str, seq_of_params: Sequence[Any]) -> None:
        futures = []
        svc = self._conn._client.sql
        for params in seq_of_params:
            futures.append(svc.execute(operation, *params))
        for fut in futures:
            fut.result()

    def fetchone(self):
        pass

    def fetchmany(self, size=None):
        pass

    def fetchall(self):
        pass

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, column=None):
        pass

    @classmethod
    def _make_description(cls, metadata: SqlRowMetadata) -> List[ResultColumn]:
        r = []
        for col in metadata.columns:
            r.append(ResultColumn(
                name=col.name, type_code=col.type,
                display_size=None, internal_size=None,
                precision=None, scale=None, null_ok=col.nullable,
            ))
        return r


class Connection:

    def __init__(self, config):
        self._client = HazelcastClient(**config)

    def close(self):
        if self._client:
            self._client.shutdown()
            self._client = None

    def commit(self):
        # transactions are not supported
        pass

    def cursor(self) -> Cursor:
        return Cursor(self)


def connect(config=None, *, user: str=None, password: str=None, host="localhost", port: int=None) -> Connection:
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