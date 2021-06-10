import itertools
import unittest
import uuid

from mock import MagicMock

from hazelcast.protocol.codec import sql_execute_codec, sql_close_codec, sql_fetch_codec
from hazelcast.protocol.client_message import _OUTBOUND_MESSAGE_MESSAGE_TYPE_OFFSET
from hazelcast.serialization import LE_INT
from hazelcast.sql import (
    SqlService,
    SqlColumnMetadata,
    SqlColumnType,
    _SqlPage,
    SqlRowMetadata,
    _InternalSqlService,
    HazelcastSqlError,
    _SqlErrorCode,
    _SqlError,
    SqlStatement,
    SqlExpectedResultType,
)

EXPECTED_ROWS = ["result", "result2"]
EXPECTED_UPDATE_COUNT = 42


class SqlMockTest(unittest.TestCase):
    def setUp(self):

        self.connection = MagicMock()

        connection_manager = MagicMock(client_uuid=uuid.uuid4())
        connection_manager.get_random_connection = MagicMock(return_value=self.connection)

        serialization_service = MagicMock()
        serialization_service.to_object.side_effect = lambda arg: arg
        serialization_service.to_data.side_effect = lambda arg: arg

        self.invocation_registry = {}
        correlation_id_counter = itertools.count()
        invocation_service = MagicMock()

        def invoke(invocation):
            self.invocation_registry[next(correlation_id_counter)] = invocation

        invocation_service.invoke.side_effect = invoke

        self.internal_service = _InternalSqlService(
            connection_manager, serialization_service, invocation_service
        )
        self.service = SqlService(self.internal_service)
        self.result = self.service.execute("SOME QUERY")

    def test_iterator_with_rows(self):
        self.set_execute_response_with_rows()
        self.assertEqual(-1, self.result.update_count().result())
        self.assertTrue(self.result.is_row_set().result())
        self.assertIsInstance(self.result.get_row_metadata().result(), SqlRowMetadata)
        self.assertEqual(EXPECTED_ROWS, self.get_rows_from_iterator())

    def test_blocking_iterator_with_rows(self):
        self.set_execute_response_with_rows()
        self.assertEqual(-1, self.result.update_count().result())
        self.assertTrue(self.result.is_row_set().result())
        self.assertIsInstance(self.result.get_row_metadata().result(), SqlRowMetadata)
        self.assertEqual(EXPECTED_ROWS, self.get_rows_from_blocking_iterator())

    def test_iterator_with_update_count(self):
        self.set_execute_response_with_update_count()
        self.assertEqual(EXPECTED_UPDATE_COUNT, self.result.update_count().result())
        self.assertFalse(self.result.is_row_set().result())

        with self.assertRaises(ValueError):
            self.result.get_row_metadata().result()

        with self.assertRaises(ValueError):
            self.result.iterator().result()

    def test_blocking_iterator_with_update_count(self):
        self.set_execute_response_with_update_count()
        self.assertEqual(EXPECTED_UPDATE_COUNT, self.result.update_count().result())
        self.assertFalse(self.result.is_row_set().result())

        with self.assertRaises(ValueError):
            self.result.get_row_metadata().result()

        with self.assertRaises(ValueError):
            for _ in self.result:
                pass

    def test_execute_error(self):
        self.set_execute_error(RuntimeError("expected"))
        with self.assertRaises(HazelcastSqlError) as cm:
            iter(self.result)

        self.assertEqual(_SqlErrorCode.GENERIC, cm.exception._code)

    def test_execute_error_when_connection_is_not_live(self):
        self.connection.live = False
        self.set_execute_error(RuntimeError("expected"))
        with self.assertRaises(HazelcastSqlError) as cm:
            iter(self.result)

        self.assertEqual(_SqlErrorCode.CONNECTION_PROBLEM, cm.exception._code)

    def test_close_when_execute_is_not_done(self):
        future = self.result.close()
        self.set_close_response()
        self.assertIsNone(future.result())
        with self.assertRaises(HazelcastSqlError) as cm:
            iter(self.result)

        self.assertEqual(_SqlErrorCode.CANCELLED_BY_USER, cm.exception._code)

    def test_close_when_close_request_fails(self):
        future = self.result.close()
        self.set_close_error(HazelcastSqlError(None, _SqlErrorCode.MAP_DESTROYED, "expected", None))

        with self.assertRaises(HazelcastSqlError) as cm:
            future.result()

        self.assertEqual(_SqlErrorCode.MAP_DESTROYED, cm.exception._code)

    def test_fetch_error(self):
        self.set_execute_response_with_rows(is_last=False)
        result = []
        i = self.result.iterator().result()
        # First page contains two rows
        result.append(next(i).result().get_object_with_index(0))
        result.append(next(i).result().get_object_with_index(0))

        self.assertEqual(EXPECTED_ROWS, result)

        # initiate the fetch request
        future = next(i)

        self.set_fetch_error(RuntimeError("expected"))

        with self.assertRaises(HazelcastSqlError) as cm:
            future.result()

        self.assertEqual(_SqlErrorCode.GENERIC, cm.exception._code)

    def test_fetch_server_error(self):
        self.set_execute_response_with_rows(is_last=False)
        result = []
        i = self.result.iterator().result()
        # First page contains two rows
        result.append(next(i).result().get_object_with_index(0))
        result.append(next(i).result().get_object_with_index(0))

        self.assertEqual(EXPECTED_ROWS, result)

        # initiate the fetch request
        future = next(i)

        self.set_fetch_response_with_error()

        with self.assertRaises(HazelcastSqlError) as cm:
            future.result()

        self.assertEqual(_SqlErrorCode.PARSING, cm.exception._code)

    def test_close_in_between_fetches(self):
        self.set_execute_response_with_rows(is_last=False)
        result = []
        i = self.result.iterator().result()
        # First page contains two rows
        result.append(next(i).result().get_object_with_index(0))
        result.append(next(i).result().get_object_with_index(0))

        self.assertEqual(EXPECTED_ROWS, result)

        # initiate the fetch request
        future = next(i)

        self.result.close()

        with self.assertRaises(HazelcastSqlError) as cm:
            future.result()

        self.assertEqual(_SqlErrorCode.CANCELLED_BY_USER, cm.exception._code)

    def set_fetch_response_with_error(self):
        response = {"row_page": None, "error": _SqlError(_SqlErrorCode.PARSING, "expected", None)}
        self.set_future_result_or_exception(response, sql_fetch_codec._REQUEST_MESSAGE_TYPE)

    def set_fetch_error(self, error):
        self.set_future_result_or_exception(error, sql_fetch_codec._REQUEST_MESSAGE_TYPE)

    def set_close_error(self, error):
        self.set_future_result_or_exception(error, sql_close_codec._REQUEST_MESSAGE_TYPE)

    def set_close_response(self):
        self.set_future_result_or_exception(None, sql_close_codec._REQUEST_MESSAGE_TYPE)

    def set_execute_response_with_update_count(self):
        self.set_execute_response(EXPECTED_UPDATE_COUNT, None, None, None)

    def get_rows_from_blocking_iterator(self):
        return [row.get_object_with_index(0) for row in self.result]

    def get_rows_from_iterator(self):
        result = []
        for row_future in self.result.iterator().result():
            try:
                row = row_future.result()
                result.append(row.get_object_with_index(0))
            except StopIteration:
                break
        return result

    def set_execute_response_with_rows(self, is_last=True):
        self.set_execute_response(
            -1,
            [SqlColumnMetadata("name", SqlColumnType.VARCHAR, True, True)],
            _SqlPage([SqlColumnType.VARCHAR], [EXPECTED_ROWS], is_last),
            None,
        )

    def set_execute_response(self, update_count, row_metadata, row_page, error):
        response = {
            "update_count": update_count,
            "row_metadata": row_metadata,
            "row_page": row_page,
            "error": error,
        }

        self.set_future_result_or_exception(response, sql_execute_codec._REQUEST_MESSAGE_TYPE)

    def set_execute_error(self, error):
        self.set_future_result_or_exception(error, sql_execute_codec._REQUEST_MESSAGE_TYPE)

    def get_message_type(self, invocation):
        return LE_INT.unpack_from(invocation.request.buf, _OUTBOUND_MESSAGE_MESSAGE_TYPE_OFFSET)[0]

    def set_future_result_or_exception(self, value, message_type):
        for invocation in self.invocation_registry.values():
            if self.get_message_type(invocation) == message_type:
                if isinstance(value, Exception):
                    invocation.future.set_exception(value)
                else:
                    invocation.future.set_result(value)


class SqlInvalidInputTest(unittest.TestCase):
    def test_statement_sql(self):
        valid_inputs = ["a", "   a", "  a  "]

        for valid in valid_inputs:
            statement = SqlStatement(valid)
            self.assertEqual(valid, statement.sql)

        invalid_inputs = ["", "   ", None, 1]

        for invalid in invalid_inputs:
            with self.assertRaises((ValueError, AssertionError)):
                SqlStatement(invalid)

    def test_statement_timeout(self):
        valid_inputs = [-1, 0, 15, 1.5]

        for valid in valid_inputs:
            statement = SqlStatement("sql")
            statement.timeout = valid
            self.assertEqual(valid, statement.timeout)

        invalid_inputs = [-10, -100, "hey", None]

        for invalid in invalid_inputs:
            statement = SqlStatement("sql")
            with self.assertRaises((ValueError, AssertionError)):
                statement.timeout = invalid

    def test_statement_cursor_buffer_size(self):
        valid_inputs = [1, 10, 999999]

        for valid in valid_inputs:
            statement = SqlStatement("something")
            statement.cursor_buffer_size = valid
            self.assertEqual(valid, statement.cursor_buffer_size)

        invalid_inputs = [0, -10, -99999, "hey", None, 1.0]

        for invalid in invalid_inputs:
            statement = SqlStatement("something")
            with self.assertRaises((ValueError, AssertionError)):
                statement.cursor_buffer_size = invalid

    def test_statement_expected_result_type(self):
        valid_inputs = [SqlExpectedResultType.ROWS, SqlExpectedResultType.UPDATE_COUNT]

        for valid in valid_inputs:
            statement = SqlStatement("something")
            statement.expected_result_type = valid
            self.assertEqual(valid, statement.expected_result_type)

        invalid_inputs = [None, 123, "hey"]

        for invalid in invalid_inputs:
            with self.assertRaises(TypeError):
                statement = SqlStatement("something")
                statement.expected_result_type = invalid
