import decimal
import math
import random
import string
import unittest

from hazelcast import six
from hazelcast.future import ImmediateFuture
from hazelcast.serialization.api import Portable
from tests.base import SingleMemberTestCase
from tests.hzrc.ttypes import Lang
from mock import patch

from tests.util import is_client_version_older_than, mark_server_version_at_least

try:
    from hazelcast.sql import HazelcastSqlError, SqlStatement, SqlExpectedResultType, SqlColumnType
except ImportError:
    # For backward compatibility. If we cannot import those, we won't
    # be even referencing them in tests.
    pass

SERVER_CONFIG = """
<hazelcast xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://www.hazelcast.com/schema/config
           http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
    <serialization>
        <portable-factories>
            <portable-factory factory-id="666">com.hazelcast.client.test.PortableFactory
            </portable-factory>
        </portable-factories>
    </serialization>
</hazelcast>
"""


class SqlTestBase(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        return SERVER_CONFIG

    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        config["portable_factories"] = {666: {6: Student}}
        return config

    def setUp(self):
        self.map_name = random_string()
        self.map = self.client.get_map(self.map_name).blocking()
        mark_server_version_at_least(self, self.client, "4.2")

    def tearDown(self):
        self.map.clear()

    def _populate_map(self, entry_count=10, value_factory=lambda v: v):
        entries = {i: value_factory(i) for i in range(entry_count)}
        self.map.put_all(entries)


@unittest.skipIf(
    is_client_version_older_than("4.2"), "Tests the features added in 4.2 version of the client"
)
class SqlServiceTest(SqlTestBase):
    def test_execute(self):
        entry_count = 11
        self._populate_map(entry_count)
        result = self.client.sql.execute("SELECT * FROM %s" % self.map_name)
        six.assertCountEqual(
            self,
            [(i, i) for i in range(entry_count)],
            [(row.get_object("__key"), row.get_object("this")) for row in result],
        )

    def test_execute_with_params(self):
        entry_count = 13
        self._populate_map(entry_count)
        result = self.client.sql.execute(
            "SELECT this FROM %s WHERE __key > ? AND this > ?" % self.map_name, 5, 6
        )
        six.assertCountEqual(
            self,
            [i for i in range(7, entry_count)],
            [row.get_object("this") for row in result],
        )

    def test_execute_with_mismatched_params_when_sql_has_more(self):
        self._populate_map()
        result = self.client.sql.execute(
            "SELECT * FROM %s WHERE __key > ? AND this > ?" % self.map_name, 5
        )

        with self.assertRaises(HazelcastSqlError):
            for _ in result:
                pass

    def test_execute_with_mismatched_params_when_params_has_more(self):
        self._populate_map()
        result = self.client.sql.execute("SELECT * FROM %s WHERE this > ?" % self.map_name, 5, 6)

        with self.assertRaises(HazelcastSqlError):
            for _ in result:
                pass

    def test_execute_statement(self):
        entry_count = 12
        self._populate_map(entry_count, str)
        statement = SqlStatement("SELECT this FROM %s" % self.map_name)
        result = self.client.sql.execute_statement(statement)

        six.assertCountEqual(
            self,
            [str(i) for i in range(entry_count)],
            [row.get_object_with_index(0) for row in result],
        )

    def test_execute_statement_with_params(self):
        entry_count = 20
        self._populate_map(entry_count, lambda v: Student(v, v))
        statement = SqlStatement(
            "SELECT age FROM %s WHERE height = CAST(? AS REAL)" % self.map_name
        )
        statement.add_parameter(13.0)
        result = self.client.sql.execute_statement(statement)

        six.assertCountEqual(self, [13], [row.get_object("age") for row in result])

    def test_execute_statement_with_mismatched_params_when_sql_has_more(self):
        self._populate_map()
        statement = SqlStatement("SELECT * FROM %s WHERE __key > ? AND this > ?" % self.map_name)
        statement.parameters = [5]
        result = self.client.sql.execute_statement(statement)

        with self.assertRaises(HazelcastSqlError):
            for _ in result:
                pass

    def test_execute_statement_with_mismatched_params_when_params_has_more(self):
        self._populate_map()
        statement = SqlStatement("SELECT * FROM %s WHERE this > ?" % self.map_name)
        statement.parameters = [5, 6]
        result = self.client.sql.execute_statement(statement)

        with self.assertRaises(HazelcastSqlError):
            for _ in result:
                pass

    def test_execute_statement_with_timeout(self):
        entry_count = 100
        self._populate_map(entry_count, lambda v: Student(v, v))
        statement = SqlStatement("SELECT age FROM %s WHERE height < 10" % self.map_name)
        statement.timeout = 100
        result = self.client.sql.execute_statement(statement)

        six.assertCountEqual(
            self, [i for i in range(10)], [row.get_object("age") for row in result]
        )

    def test_execute_statement_with_cursor_buffer_size(self):
        entry_count = 50
        self._populate_map(entry_count, lambda v: Student(v, v))
        statement = SqlStatement("SELECT age FROM %s" % self.map_name)
        statement.cursor_buffer_size = 3
        result = self.client.sql.execute_statement(statement)

        with patch.object(result, "_fetch_next_page", wraps=result._fetch_next_page) as patched:
            six.assertCountEqual(
                self, [i for i in range(entry_count)], [row.get_object("age") for row in result]
            )
            # -1 comes from the fact that, we don't fetch the first page
            self.assertEqual(
                math.ceil(float(entry_count) / statement.cursor_buffer_size) - 1, patched.call_count
            )

    def test_execute_statement_with_copy(self):
        self._populate_map()
        statement = SqlStatement("SELECT __key FROM %s WHERE this >= ?" % self.map_name)
        statement.parameters = [9]
        copy_statement = statement.copy()
        statement.clear_parameters()

        result = self.client.sql.execute_statement(copy_statement)
        self.assertEqual([9], [row.get_object_with_index(0) for row in result])

        result = self.client.sql.execute_statement(statement)
        with self.assertRaises(HazelcastSqlError):
            for _ in result:
                pass

    # Can't test the case we would expect an update count, because the IMDG SQL
    # engine does not support such query as of now.
    def test_execute_statement_with_expected_result_type_of_rows_when_rows_are_expected(self):
        entry_count = 100
        self._populate_map(entry_count, lambda v: Student(v, v))
        statement = SqlStatement("SELECT age FROM %s WHERE age < 3" % self.map_name)
        statement.expected_result_type = SqlExpectedResultType.ROWS
        result = self.client.sql.execute_statement(statement)

        six.assertCountEqual(self, [i for i in range(3)], [row.get_object("age") for row in result])

    # Can't test the case we would expect an update count, because the IMDG SQL
    # engine does not support such query as of now.
    def test_execute_statement_with_expected_result_type_of_update_count_when_rows_are_expected(
        self,
    ):
        self._populate_map()
        statement = SqlStatement("SELECT * FROM %s" % self.map_name)
        statement.expected_result_type = SqlExpectedResultType.UPDATE_COUNT
        result = self.client.sql.execute_statement(statement)

        with self.assertRaises(HazelcastSqlError):
            for _ in result:
                pass

    # Can't test the schema, because the IMDG SQL engine does not support
    # specifying a schema yet.


@unittest.skipIf(
    is_client_version_older_than("4.2"), "Tests the features added in 4.2 version of the client"
)
class SqlResultTest(SqlTestBase):
    def test_blocking_iterator(self):
        self._populate_map()
        result = self.client.sql.execute("SELECT __key FROM %s" % self.map_name)

        six.assertCountEqual(
            self, [i for i in range(10)], [row.get_object_with_index(0) for row in result]
        )

    def test_blocking_iterator_when_iterator_requested_more_than_once(self):
        self._populate_map()
        result = self.client.sql.execute("SELECT this FROM %s" % self.map_name)

        six.assertCountEqual(
            self, [i for i in range(10)], [row.get_object_with_index(0) for row in result]
        )

        with self.assertRaises(ValueError):
            for _ in result:
                pass

    def test_blocking_iterator_with_multi_paged_result(self):
        self._populate_map()
        statement = SqlStatement("SELECT __key FROM %s" % self.map_name)
        statement.cursor_buffer_size = 1  # Each page will contain just 1 result
        result = self.client.sql.execute_statement(statement)

        six.assertCountEqual(
            self, [i for i in range(10)], [row.get_object_with_index(0) for row in result]
        )

    def test_iterator(self):
        self._populate_map()
        result = self.client.sql.execute("SELECT __key FROM %s" % self.map_name)

        iterator_future = result.iterator()

        rows = []

        def cb(f):
            iterator = f.result()

            def iterate(row_future):
                try:
                    row = row_future.result()
                    rows.append(row.get_object_with_index(0))
                    next(iterator).add_done_callback(iterate)
                except StopIteration:
                    pass

            next(iterator).add_done_callback(iterate)

        iterator_future.add_done_callback(cb)

        def assertion():
            six.assertCountEqual(
                self,
                [i for i in range(10)],
                rows,
            )

        self.assertTrueEventually(assertion)

    def test_iterator_when_iterator_requested_more_than_once(self):
        self._populate_map()
        result = self.client.sql.execute("SELECT this FROM %s" % self.map_name)

        iterator = result.iterator().result()

        rows = []
        for row_future in iterator:
            try:
                row = row_future.result()
                rows.append(row.get_object("this"))
            except StopIteration:
                break

        six.assertCountEqual(self, [i for i in range(10)], rows)

        with self.assertRaises(ValueError):
            result.iterator().result()

    def test_iterator_with_multi_paged_result(self):
        self._populate_map()
        statement = SqlStatement("SELECT __key FROM %s" % self.map_name)
        statement.cursor_buffer_size = 1  # Each page will contain just 1 result
        result = self.client.sql.execute_statement(statement)

        iterator = result.iterator().result()

        rows = []
        for row_future in iterator:
            try:
                row = row_future.result()
                rows.append(row.get_object_with_index(0))
            except StopIteration:
                break

        six.assertCountEqual(self, [i for i in range(10)], rows)

    def test_request_blocking_iterator_after_iterator(self):
        self._populate_map()
        result = self.client.sql.execute("SELECT * FROM %s" % self.map_name)

        result.iterator().result()

        with self.assertRaises(ValueError):
            for _ in result:
                pass

    def test_request_iterator_after_blocking_iterator(self):
        self._populate_map()
        result = self.client.sql.execute("SELECT * FROM %s" % self.map_name)

        for _ in result:
            pass

        with self.assertRaises(ValueError):
            result.iterator().result()

    # Can't test the case we would expect row to be not set, because the IMDG SQL
    # engine does not support update/insert queries now.
    def test_is_row_set_when_row_is_set(self):
        self._populate_map()
        result = self.client.sql.execute("SELECT * FROM %s" % self.map_name)
        self.assertTrue(result.is_row_set().result())

    # Can't test the case we would expect a non-negative updated count, because the IMDG SQL
    # engine does not support update/insert queries now.
    def test_update_count_when_there_is_no_update(self):
        self._populate_map()
        result = self.client.sql.execute("SELECT * FROM %s WHERE __key > 5" % self.map_name)
        self.assertEqual(-1, result.update_count().result())

    def test_get_row_metadata(self):
        self._populate_map(value_factory=str)
        result = self.client.sql.execute("SELECT __key, this FROM %s" % self.map_name)
        row_metadata = result.get_row_metadata().result()
        self.assertEqual(2, row_metadata.column_count)
        columns = row_metadata.columns
        self.assertEqual(SqlColumnType.INTEGER, columns[0].type)
        self.assertEqual(SqlColumnType.VARCHAR, columns[1].type)
        self.assertTrue(columns[0].nullable)
        self.assertTrue(columns[1].nullable)

    def test_close_after_query_execution(self):
        self._populate_map()
        result = self.client.sql.execute("SELECT * FROM %s" % self.map_name)
        for _ in result:
            pass
        self.assertIsNone(result.close().result())

    def test_close_when_query_is_active(self):
        self._populate_map()
        statement = SqlStatement("SELECT * FROM %s " % self.map_name)
        statement.cursor_buffer_size = 1  # Each page will contain 1 row
        result = self.client.sql.execute_statement(statement)

        # Fetch couple of pages
        iterator = iter(result)
        next(iterator)

        self.assertIsNone(result.close().result())

        with self.assertRaises(HazelcastSqlError):
            # Next fetch requests should fail
            next(iterator)

    def test_with_statement(self):
        self._populate_map()
        with self.client.sql.execute("SELECT this FROM %s" % self.map_name) as result:
            six.assertCountEqual(
                self, [i for i in range(10)], [row.get_object_with_index(0) for row in result]
            )

    def test_with_statement_when_iteration_throws(self):
        self._populate_map()
        statement = SqlStatement("SELECT this FROM %s" % self.map_name)
        statement.cursor_buffer_size = 1  # so that it doesn't close immediately

        with self.assertRaises(RuntimeError):
            with self.client.sql.execute_statement(statement) as result:
                for _ in result:
                    raise RuntimeError("expected")

        self.assertIsInstance(result.close(), ImmediateFuture)


@unittest.skipIf(
    is_client_version_older_than("4.2"), "Tests the features added in 4.2 version of the client"
)
class SqlColumnTypesReadTest(SqlTestBase):
    def test_varchar(self):
        def value_factory(key):
            return "val-%s" % key

        self._populate_map(value_factory=value_factory)
        self._validate_rows(SqlColumnType.VARCHAR, value_factory)

    def test_boolean(self):
        def value_factory(key):
            return key % 2 == 0

        self._populate_map(value_factory=value_factory)
        self._validate_rows(SqlColumnType.BOOLEAN, value_factory)

    def test_tiny_int(self):
        self._populate_map_via_rc("new java.lang.Byte(key)")
        self._validate_rows(SqlColumnType.TINYINT)

    def test_small_int(self):
        self._populate_map_via_rc("new java.lang.Short(key)")
        self._validate_rows(SqlColumnType.SMALLINT)

    def test_integer(self):
        self._populate_map_via_rc("new java.lang.Integer(key)")
        self._validate_rows(SqlColumnType.INTEGER)

    def test_big_int(self):
        self._populate_map_via_rc("new java.lang.Long(key)")
        self._validate_rows(SqlColumnType.BIGINT)

    def test_real(self):
        self._populate_map_via_rc("new java.lang.Float(key * 1.0 / 8)")
        self._validate_rows(SqlColumnType.REAL, lambda x: x * 1.0 / 8)

    def test_double(self):
        self._populate_map_via_rc("new java.lang.Double(key * 1.0 / 1.1)")
        self._validate_rows(SqlColumnType.DOUBLE, lambda x: x * 1.0 / 1.1)

    def test_date(self):
        def value_factory(key):
            return "%d-%02d-%02d" % (key + 2000, key + 1, key + 1)

        self._populate_map_via_rc("java.time.LocalDate.of(key + 2000, key + 1, key + 1)")
        self._validate_rows(SqlColumnType.DATE, value_factory)

    def test_time(self):
        def value_factory(key):
            time = "%02d:%02d:%02d" % (key, key, key)
            if key != 0:
                time += ".%06d" % key
            return time

        self._populate_map_via_rc("java.time.LocalTime.of(key, key, key, key * 1000)")
        self._validate_rows(SqlColumnType.TIME, value_factory)

    def test_timestamp(self):
        def value_factory(key):
            timestamp = "%d-%02d-%02dT%02d:%02d:%02d" % (
                key + 2000,
                key + 1,
                key + 1,
                key,
                key,
                key,
            )
            if key != 0:
                timestamp += ".%06d" % key

            return timestamp

        self._populate_map_via_rc(
            "java.time.LocalDateTime.of(key + 2000, key + 1, key + 1, key, key, key, key * 1000)"
        )
        self._validate_rows(SqlColumnType.TIMESTAMP, value_factory)

    def test_timestamp_with_time_zone(self):
        def value_factory(key):
            timestamp = "%d-%02d-%02dT%02d:%02d:%02d" % (
                key + 2000,
                key + 1,
                key + 1,
                key,
                key,
                key,
            )
            if key != 0:
                timestamp += ".%06d" % key

            return timestamp + "+%02d:00" % key

        self._populate_map_via_rc(
            "java.time.OffsetDateTime.of(key + 2000, key + 1, key + 1, key, key, key, key * 1000, "
            "java.time.ZoneOffset.ofHours(key))"
        )
        self._validate_rows(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, value_factory)

    def test_decimal(self):
        def value_factory(key):
            return str(decimal.Decimal((0, (key,), -1 * key)))

        self._populate_map_via_rc("java.math.BigDecimal.valueOf(key, key)")
        self._validate_rows(SqlColumnType.DECIMAL, value_factory)

    def test_null(self):
        self._populate_map()
        result = self.client.sql.execute("SELECT __key, NULL AS this FROM %s" % self.map_name)
        self._validate_result(result, SqlColumnType.NULL, lambda _: None)

    def test_object(self):
        def value_factory(key):
            return Student(key, key)

        self._populate_map(value_factory=value_factory)
        result = self.client.sql.execute("SELECT __key, this FROM %s" % self.map_name)
        self._validate_result(result, SqlColumnType.OBJECT, value_factory)

    def test_null_only_column(self):
        self._populate_map()
        result = self.client.sql.execute(
            "SELECT __key, CAST(NULL AS INTEGER) as this FROM %s" % self.map_name
        )
        self._validate_result(result, SqlColumnType.INTEGER, lambda _: None)

    def _validate_rows(self, expected_type, value_factory=lambda key: key):
        result = self.client.sql.execute("SELECT __key, this FROM %s " % self.map_name)
        self._validate_result(result, expected_type, value_factory)

    def _validate_result(self, result, expected_type, factory):
        for row in result:
            key = row.get_object("__key")
            expected_value = factory(key)
            row_metadata = row.metadata

            self.assertEqual(2, row_metadata.column_count)
            column_metadata = row_metadata.get_column(1)
            self.assertEqual(expected_type, column_metadata.type)
            self.assertEqual(expected_value, row.get_object("this"))

    def _populate_map_via_rc(self, new_object_literal):
        script = """
        var map = instance_0.getMap("%s");
        for (var key = 0; key < 10; key++) {
            map.set(new java.lang.Integer(key), %s);
        }
        """ % (
            self.map_name,
            new_object_literal,
        )

        response = self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT)
        self.assertTrue(response.success)


LITE_MEMBER_CONFIG = """
<hazelcast xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://www.hazelcast.com/schema/config
           http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
    <lite-member enabled="true" />
</hazelcast>
"""


@unittest.skipIf(
    is_client_version_older_than("4.2"), "Tests the features added in 4.2 version of the client"
)
class SqlServiceLiteMemberClusterTest(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        return LITE_MEMBER_CONFIG

    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    def setUp(self):
        mark_server_version_at_least(self, self.client, "4.2")

    def test_execute(self):
        with self.assertRaises(HazelcastSqlError) as cm:
            self.client.sql.execute("SOME QUERY")

        # Make sure that exception is originating from the client
        self.assertNotEqual(self.member.uuid, str(cm.exception.originating_member_uuid))

    def test_execute_statement(self):
        statement = SqlStatement("SOME QUERY")
        with self.assertRaises(HazelcastSqlError) as cm:
            self.client.sql.execute_statement(statement)

        # Make sure that exception is originating from the client
        self.assertNotEqual(self.member.uuid, str(cm.exception.originating_member_uuid))


class Student(Portable):
    def __init__(self, age=None, height=None):
        self.age = age
        self.height = height

    def write_portable(self, writer):
        writer.write_long("age", self.age)
        writer.write_float("height", self.height)

    def read_portable(self, reader):
        self.age = reader.read_long("age")
        self.height = reader.read_float("height")

    def get_factory_id(self):
        return 666

    def get_class_id(self):
        return 6

    def __eq__(self, other):
        return isinstance(other, Student) and self.age == other.age and self.height == other.height


def random_string():
    return "".join(random.choice(string.ascii_letters) for _ in range(random.randint(3, 20)))
