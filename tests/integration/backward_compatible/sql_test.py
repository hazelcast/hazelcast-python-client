import datetime
import decimal
import math
import random
import string
import unittest

from hazelcast import HazelcastClient
from hazelcast.future import ImmediateFuture
from hazelcast.serialization.api import Portable
from tests.base import SingleMemberTestCase, HazelcastTestCase
from tests.hzrc.ttypes import Lang
from mock import patch

from tests.util import (
    compare_server_version_with_rc,
    compare_client_version,
    skip_if_server_version_older_than,
    skip_if_server_version_newer_than_or_equal,
    skip_if_client_version_older_than,
)

try:
    from hazelcast.sql import HazelcastSqlError, SqlExpectedResultType, SqlColumnType
except ImportError:
    # For backward compatibility. If we cannot import those, we won't
    # be even referencing them in tests.
    pass

try:
    from hazelcast.sql import SqlStatement
except ImportError:
    # For backward compatibility with 4.x clients.
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
    </serialization>%s
</hazelcast>
"""

JET_ENABLED_CONFIG = """
    <jet enabled="true" />
"""


class SqlTestBase(HazelcastTestCase):

    rc = None
    cluster = None
    is_v5_or_newer_server = None
    is_v5_or_newer_client = None
    client = None

    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.is_v5_or_newer_server = compare_server_version_with_rc(cls.rc, "5.0") >= 0
        cls.is_v5_or_newer_client = compare_client_version("5.0") >= 0
        # enable Jet if the server is 5.0+
        cluster_config = SERVER_CONFIG % (JET_ENABLED_CONFIG if cls.is_v5_or_newer_server else "")
        cls.cluster = cls.create_cluster(cls.rc, cluster_config)
        cls.member = cls.cluster.start_member()
        cls.client = HazelcastClient(
            cluster_name=cls.cluster.id, portable_factories={666: {6: Student}}
        )

    @classmethod
    def tearDownClass(cls):
        cls.client.shutdown()
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    def setUp(self):
        self.map_name = random_string()
        self.map = self.client.get_map(self.map_name).blocking()
        self._setup_skip_condition()

        # Skip tests if major versions of the client/server do not match.
        if self.is_v5_or_newer_client != self.is_v5_or_newer_server:
            self.skipTest("Major versions of the client and the server do not match.")

    def _setup_skip_condition(self):
        skip_if_server_version_older_than(self, self.client, "4.2")

    def tearDown(self):
        self.map.clear()

    def _populate_map(self, entry_count=10, value_factory=lambda v: v):
        entries = {i: value_factory(i) for i in range(entry_count)}
        self.map.put_all(entries)

    def _create_mapping(self, value_format="INTEGER"):
        if not self.is_v5_or_newer_server:
            # Implicit mappings are removed in 5.0
            return

        create_mapping_query = """
        CREATE MAPPING "%s" (
            __key INT,
            this %s
        )
        TYPE IMaP 
        OPTIONS (
            'keyFormat' = 'int',
            'valueFormat' = '%s'
        )
        """ % (
            self.map_name,
            value_format,
            value_format.lower(),
        )

        self.execute(create_mapping_query)

    def _create_mapping_for_portable(self, factory_id, class_id, columns):
        if not self.is_v5_or_newer_server:
            # Implicit mappings are removed in 5.0
            return

        create_mapping_query = """
        CREATE MAPPING "%s" (
            __key INT%s
            %s
        )
        TYPE IMaP 
        OPTIONS (
            'keyFormat' = 'int',
            'valueFormat' = 'portable',
            'valuePortableFactoryId' = '%s',
            'valuePortableClassId' = '%s'
        )
        """ % (
            self.map_name,
            "," if len(columns) > 0 else "",
            ",\n".join(["%s %s" % (c_name, c_type) for c_name, c_type in columns.items()]),
            factory_id,
            class_id,
        )

        self.execute(create_mapping_query)

    def execute(self, query, *args):
        if self.is_v5_or_newer_client:
            return self.client.sql.execute(query, *args).result()

        # Compatibility with 4.x clients
        return self.client.sql.execute(query, *args)

    def execute_statement(self, query, *args, **kwargs):
        if self.is_v5_or_newer_client:
            return self.client.sql.execute(query, *args, **kwargs).result()

        # Compatibility with 4.x clients
        statement = SqlStatement(query)
        for arg in args:
            statement.add_parameter(arg)

        for key, value in kwargs.items():
            setattr(statement, key, value)

        return self.client.sql.execute_statement(statement)

    def update_count(self, result):
        if self.is_v5_or_newer_client:
            return result.update_count()

        # Compatibility with 4.x clients
        return result.update_count().result()

    def iterator(self, result):
        if self.is_v5_or_newer_client:
            return result.iterator()

        # Compatibility with 4.x clients
        return result.iterator().result()

    def is_row_set(self, result):
        if self.is_v5_or_newer_client:
            return result.is_row_set()

        # Compatibility with 4.x clients
        return result.is_row_set().result()

    def get_row_metadata(self, result):
        if self.is_v5_or_newer_client:
            return result.get_row_metadata()

        # Compatibility with 4.x clients
        return result.get_row_metadata().result()


@unittest.skipIf(
    compare_client_version("4.2") < 0, "Tests the features added in 4.2 version of the client"
)
class SqlServiceTest(SqlTestBase):
    def test_execute(self):
        self._create_mapping()
        entry_count = 11
        self._populate_map(entry_count)
        result = self.execute('SELECT * FROM "%s"' % self.map_name)
        self.assertCountEqual(
            [(i, i) for i in range(entry_count)],
            [(row.get_object("__key"), row.get_object("this")) for row in result],
        )

    def test_execute_with_params(self):
        self._create_mapping()
        entry_count = 13
        self._populate_map(entry_count)
        result = self.execute(
            'SELECT this FROM "%s" WHERE __key > ? AND this > ?' % self.map_name, 5, 6
        )
        self.assertCountEqual(
            [i for i in range(7, entry_count)],
            [row.get_object("this") for row in result],
        )

    def test_execute_with_mismatched_params_when_sql_has_more(self):
        self._create_mapping()
        self._populate_map()

        with self.assertRaises(HazelcastSqlError):
            result = self.execute(
                'SELECT * FROM "%s" WHERE __key > ? AND this > ?' % self.map_name, 5
            )
            for _ in result:
                pass

    def test_execute_with_mismatched_params_when_params_has_more(self):
        self._create_mapping()
        self._populate_map()

        with self.assertRaises(HazelcastSqlError):
            result = self.execute('SELECT * FROM "%s" WHERE this > ?' % self.map_name, 5, 6)
            for _ in result:
                pass

    def test_execute_statement(self):
        self._create_mapping("VARCHAR")
        entry_count = 12
        self._populate_map(entry_count, str)
        result = self.execute_statement('SELECT this FROM "%s"' % self.map_name)

        self.assertCountEqual(
            [str(i) for i in range(entry_count)],
            [row.get_object_with_index(0) for row in result],
        )

    def test_execute_statement_with_params(self):
        self._create_mapping_for_portable(666, 6, {"age": "BIGINT", "height": "REAL"})
        entry_count = 20
        self._populate_map(entry_count, lambda v: Student(v, v))
        result = self.execute_statement(
            'SELECT age FROM "%s" WHERE height = CAST(? AS REAL)' % self.map_name,
            13.0,
        )

        self.assertCountEqual([13], [row.get_object("age") for row in result])

    def test_execute_statement_with_mismatched_params_when_sql_has_more(self):
        self._create_mapping()
        self._populate_map()

        with self.assertRaises(HazelcastSqlError):
            result = self.execute_statement(
                'SELECT * FROM "%s" WHERE __key > ? AND this > ?' % self.map_name,
                5,
            )
            for _ in result:
                pass

    def test_execute_statement_with_mismatched_params_when_params_has_more(self):
        self._create_mapping()
        self._populate_map()

        with self.assertRaises(HazelcastSqlError):
            result = self.execute_statement(
                'SELECT * FROM "%s" WHERE this > ?' % self.map_name,
                5,
                6,
            )
            for _ in result:
                pass

    def test_execute_statement_with_timeout(self):
        self._create_mapping_for_portable(666, 6, {"age": "BIGINT", "height": "REAL"})
        entry_count = 100
        self._populate_map(entry_count, lambda v: Student(v, v))
        result = self.execute_statement(
            'SELECT age FROM "%s" WHERE height < 10' % self.map_name,
            timeout=100,
        )

        self.assertCountEqual([i for i in range(10)], [row.get_object("age") for row in result])

    def test_execute_statement_with_cursor_buffer_size(self):
        self._create_mapping_for_portable(666, 6, {"age": "BIGINT", "height": "REAL"})
        entry_count = 50
        self._populate_map(entry_count, lambda v: Student(v, v))
        result = self.execute_statement(
            'SELECT age FROM "%s"' % self.map_name,
            cursor_buffer_size=3,
        )

        with patch.object(result, "_fetch_next_page", wraps=result._fetch_next_page) as patched:
            self.assertCountEqual(
                [i for i in range(entry_count)], [row.get_object("age") for row in result]
            )
            # -1 comes from the fact that, we don't fetch the first page.
            expected = math.ceil(entry_count / 3.0) - 1
            actual = patched.call_count
            self.assertEqual(expected, actual)

    # Can't test the case we would expect an update count, because the IMDG SQL
    # engine does not support such query as of now.
    def test_execute_statement_with_expected_result_type_of_rows_when_rows_are_expected(self):
        self._create_mapping_for_portable(666, 6, {"age": "BIGINT", "height": "REAL"})
        entry_count = 100
        self._populate_map(entry_count, lambda v: Student(v, v))
        result = self.execute_statement(
            'SELECT age FROM "%s" WHERE age < 3' % self.map_name,
            expected_result_type=SqlExpectedResultType.ROWS,
        )

        self.assertCountEqual([i for i in range(3)], [row.get_object("age") for row in result])

    # Can't test the case we would expect an update count, because the IMDG SQL
    # engine does not support such query as of now.
    def test_execute_statement_with_expected_result_type_of_update_count_when_rows_are_expected(
        self,
    ):
        self._create_mapping()
        self._populate_map()

        with self.assertRaises(HazelcastSqlError):
            result = self.execute_statement(
                'SELECT * FROM "%s"' % self.map_name,
                expected_result_type=SqlExpectedResultType.UPDATE_COUNT,
            )
            for _ in result:
                pass

    # Can't test the schema, because the IMDG SQL engine does not support
    # specifying a schema yet.

    def test_provided_suggestions(self):
        skip_if_client_version_older_than(self, "5.0")
        skip_if_server_version_older_than(self, self.client, "5.0")

        # We don't create a mapping intentionally to get suggestions
        self.map.put(1, "value-1")
        select_all_query = 'SELECT * FROM "%s"' % self.map_name
        with self.assertRaises(HazelcastSqlError) as cm:
            self.execute(select_all_query)

        self.execute(cm.exception.suggestion)

        with self.execute(select_all_query) as result:
            self.assertEqual(
                [(1, "value-1")],
                [(r.get_object("__key"), r.get_object("this")) for r in result],
            )


@unittest.skipIf(
    compare_client_version("4.2") < 0, "Tests the features added in 4.2 version of the client"
)
class SqlResultTest(SqlTestBase):
    def test_blocking_iterator(self):
        self._create_mapping()
        self._populate_map()
        result = self.execute('SELECT __key FROM "%s"' % self.map_name)

        self.assertCountEqual(
            [i for i in range(10)], [row.get_object_with_index(0) for row in result]
        )

    def test_blocking_iterator_when_iterator_requested_more_than_once(self):
        self._create_mapping()
        self._populate_map()
        result = self.execute('SELECT this FROM "%s"' % self.map_name)

        self.assertCountEqual(
            [i for i in range(10)], [row.get_object_with_index(0) for row in result]
        )

        with self.assertRaises(ValueError):
            for _ in result:
                pass

    def test_blocking_iterator_with_multi_paged_result(self):
        self._create_mapping()
        self._populate_map()
        # Each page will contain just 1 result
        result = self.execute_statement(
            'SELECT __key FROM "%s"' % self.map_name,
            cursor_buffer_size=1,
        )

        self.assertCountEqual(
            [i for i in range(10)], [row.get_object_with_index(0) for row in result]
        )

    def test_iterator(self):
        self._create_mapping()
        self._populate_map()
        result = self.execute('SELECT __key FROM "%s"' % self.map_name)

        iterator = self.iterator(result)

        rows = []

        def iterate(row_future):
            try:
                row = row_future.result()
                rows.append(row.get_object_with_index(0))
                next(iterator).add_done_callback(iterate)
            except StopIteration:
                pass

        next(iterator).add_done_callback(iterate)

        def assertion():
            self.assertCountEqual(
                [i for i in range(10)],
                rows,
            )

        self.assertTrueEventually(assertion)

    def test_iterator_when_iterator_requested_more_than_once(self):
        self._create_mapping()
        self._populate_map()
        result = self.execute('SELECT this FROM "%s"' % self.map_name)

        iterator = self.iterator(result)

        rows = []
        for row_future in iterator:
            try:
                row = row_future.result()
                rows.append(row.get_object("this"))
            except StopIteration:
                break

        self.assertCountEqual([i for i in range(10)], rows)

        with self.assertRaises(ValueError):
            self.iterator(result)

    def test_iterator_with_multi_paged_result(self):
        self._create_mapping()
        self._populate_map()
        # Each page will contain just 1 result
        result = self.execute_statement(
            'SELECT __key FROM "%s"' % self.map_name,
            cursor_buffer_size=1,
        )

        iterator = self.iterator(result)

        rows = []
        for row_future in iterator:
            try:
                row = row_future.result()
                rows.append(row.get_object_with_index(0))
            except StopIteration:
                break

        self.assertCountEqual([i for i in range(10)], rows)

    def test_request_blocking_iterator_after_iterator(self):
        self._create_mapping()
        self._populate_map()
        result = self.execute('SELECT * FROM "%s"' % self.map_name)

        self.iterator(result)

        with self.assertRaises(ValueError):
            for _ in result:
                pass

    def test_request_iterator_after_blocking_iterator(self):
        self._create_mapping()
        self._populate_map()
        result = self.execute('SELECT * FROM "%s"' % self.map_name)

        for _ in result:
            pass

        with self.assertRaises(ValueError):
            self.iterator(result)

    # Can't test the case we would expect row to be not set, because the IMDG SQL
    # engine does not support update/insert queries now.
    def test_is_row_set_when_row_is_set(self):
        self._create_mapping()
        self._populate_map()
        result = self.execute('SELECT * FROM "%s"' % self.map_name)
        self.assertTrue(self.is_row_set(result))

    # Can't test the case we would expect a non-negative updated count, because the IMDG SQL
    # engine does not support update/insert queries now.
    def test_update_count_when_there_is_no_update(self):
        self._create_mapping()
        self._populate_map()
        result = self.execute('SELECT * FROM "%s" WHERE __key > 5' % self.map_name)
        self.assertEqual(-1, self.update_count(result))

    def test_get_row_metadata(self):
        self._create_mapping("VARCHAR")
        self._populate_map(value_factory=str)
        result = self.execute('SELECT __key, this FROM "%s"' % self.map_name)
        row_metadata = self.get_row_metadata(result)
        self.assertEqual(2, row_metadata.column_count)
        columns = row_metadata.columns
        self.assertEqual(SqlColumnType.INTEGER, columns[0].type)
        self.assertEqual(SqlColumnType.VARCHAR, columns[1].type)
        self.assertTrue(columns[0].nullable)
        self.assertTrue(columns[1].nullable)

    def test_close_after_query_execution(self):
        self._create_mapping()
        self._populate_map()
        result = self.execute('SELECT * FROM "%s"' % self.map_name)
        for _ in result:
            pass

        self.assertIsNone(result.close().result())

    def test_close_when_query_is_active(self):
        self._create_mapping()
        self._populate_map()
        # Each page will contain 1 row
        result = self.execute_statement(
            'SELECT * FROM "%s"' % self.map_name,
            cursor_buffer_size=1,
        )

        # Fetch couple of pages
        iterator = iter(result)
        next(iterator)

        self.assertIsNone(result.close().result())

        with self.assertRaises(HazelcastSqlError):
            # Next fetch requests should fail
            next(iterator)

    def test_with_statement(self):
        self._create_mapping()
        self._populate_map()
        with self.execute('SELECT this FROM "%s"' % self.map_name) as result:
            self.assertCountEqual(
                [i for i in range(10)], [row.get_object_with_index(0) for row in result]
            )

    def test_with_statement_when_iteration_throws(self):
        self._create_mapping()
        self._populate_map()

        with self.assertRaises(RuntimeError):
            with self.execute_statement(
                'SELECT this FROM "%s"' % self.map_name,
                cursor_buffer_size=1,  # so that it doesn't close immediately
            ) as result:
                for _ in result:
                    raise RuntimeError("expected")

        self.assertIsInstance(result.close(), ImmediateFuture)

    def test_lazy_deserialization(self):
        skip_if_client_version_older_than(self, "5.0")

        # Using a Portable that is not defined on the client-side.
        self._create_mapping_for_portable(666, 1, {})

        script = (
            """
        var m = instance_0.getMap("%s");
        m.put(1, new com.hazelcast.client.test.Employee(1, "Joe"));
        """
            % self.map_name
        )

        res = self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT)
        self.assertTrue(res.success)

        with self.execute('SELECT __key, this FROM "%s"' % self.map_name) as result:
            rows = list(result)
            self.assertEqual(1, len(rows))
            row = rows[0]
            # We should be able to deserialize parts of the response
            self.assertEqual(1, row.get_object("__key"))

            # We should throw lazily when we try to access the columns
            # that are not deserializable
            with self.assertRaises(HazelcastSqlError):
                row.get_object("this")

    def test_rows_as_dict_or_list(self):
        skip_if_client_version_older_than(self, "5.0")

        self._create_mapping("VARCHAR")
        entry_count = 20

        def value_factory(v):
            return "value-%s" % v

        self._populate_map(entry_count, value_factory)

        expected = [(i, value_factory(i)) for i in range(entry_count)]
        with self.execute('SELECT __key, this FROM "%s"' % self.map_name) as result:
            # Verify that both row[integer] and row[string] works
            self.assertCountEqual(expected, [(row[0], row["this"]) for row in result])


@unittest.skipIf(
    compare_client_version("4.2") < 0, "Tests the features added in 4.2 version of the client"
)
class SqlColumnTypesReadTest(SqlTestBase):
    def test_varchar(self):
        def value_factory(key):
            return "val-%s" % key

        self._create_mapping("VARCHAR")
        self._populate_map(value_factory=value_factory)
        self._validate_rows(SqlColumnType.VARCHAR, value_factory)

    def test_boolean(self):
        def value_factory(key):
            return key % 2 == 0

        self._create_mapping("BOOLEAN")
        self._populate_map(value_factory=value_factory)
        self._validate_rows(SqlColumnType.BOOLEAN, value_factory)

    def test_tiny_int(self):
        self._create_mapping("TINYINT")
        self._populate_map_via_rc("new java.lang.Byte(key)")
        self._validate_rows(SqlColumnType.TINYINT)

    def test_small_int(self):
        self._create_mapping("SMALLINT")
        self._populate_map_via_rc("new java.lang.Short(key)")
        self._validate_rows(SqlColumnType.SMALLINT)

    def test_integer(self):
        self._create_mapping("INTEGER")
        self._populate_map_via_rc("new java.lang.Integer(key)")
        self._validate_rows(SqlColumnType.INTEGER)

    def test_big_int(self):
        self._create_mapping("BIGINT")
        self._populate_map_via_rc("new java.lang.Long(key)")
        self._validate_rows(SqlColumnType.BIGINT)

    def test_real(self):
        self._create_mapping("REAL")
        self._populate_map_via_rc("new java.lang.Float(key * 1.0 / 8)")
        self._validate_rows(SqlColumnType.REAL, lambda x: x * 1.0 / 8)

    def test_double(self):
        self._create_mapping("DOUBLE")
        self._populate_map_via_rc("new java.lang.Double(key * 1.0 / 1.1)")
        self._validate_rows(SqlColumnType.DOUBLE, lambda x: x * 1.0 / 1.1)

    def test_date(self):
        def value_factory(key):
            if self.is_v5_or_newer_server:
                return datetime.date(key + 2000, key + 1, key + 1)

            return "%d-%02d-%02d" % (key + 2000, key + 1, key + 1)

        self._create_mapping("DATE")
        self._populate_map_via_rc("java.time.LocalDate.of(key + 2000, key + 1, key + 1)")
        self._validate_rows(SqlColumnType.DATE, value_factory)

    def test_time(self):
        def value_factory(key):
            if self.is_v5_or_newer_server:
                return datetime.time(key, key, key, key)

            time = "%02d:%02d:%02d" % (key, key, key)
            if key != 0:
                time += ".%06d" % key
            return time

        self._create_mapping("TIME")
        self._populate_map_via_rc("java.time.LocalTime.of(key, key, key, key * 1000)")
        self._validate_rows(SqlColumnType.TIME, value_factory)

    def test_timestamp(self):
        def value_factory(key):
            if self.is_v5_or_newer_server:
                return datetime.datetime(key + 2000, key + 1, key + 1, key, key, key, key)

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

        self._create_mapping("TIMESTAMP")
        self._populate_map_via_rc(
            "java.time.LocalDateTime.of(key + 2000, key + 1, key + 1, key, key, key, key * 1000)"
        )
        self._validate_rows(SqlColumnType.TIMESTAMP, value_factory)

    def test_timestamp_with_time_zone(self):
        def value_factory(key):
            if self.is_v5_or_newer_server:
                return datetime.datetime(
                    key + 2000,
                    key + 1,
                    key + 1,
                    key,
                    key,
                    key,
                    key,
                    datetime.timezone(datetime.timedelta(hours=key)),
                )

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

        self._create_mapping("TIMESTAMP WITH TIME ZONE")
        self._populate_map_via_rc(
            "java.time.OffsetDateTime.of(key + 2000, key + 1, key + 1, key, key, key, key * 1000, "
            "java.time.ZoneOffset.ofHours(key))"
        )
        self._validate_rows(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, value_factory)

    def test_decimal(self):
        def value_factory(key):
            d = decimal.Decimal((0, (key,), -1 * key))
            if self.is_v5_or_newer_server:
                return d

            return str(d)

        self._create_mapping("DECIMAL")
        self._populate_map_via_rc("java.math.BigDecimal.valueOf(key, key)")
        self._validate_rows(SqlColumnType.DECIMAL, value_factory)

    def test_null(self):
        self._create_mapping("INTEGER")
        self._populate_map()
        result = self.execute('SELECT __key, NULL AS this FROM "%s"' % self.map_name)
        self._validate_result(result, SqlColumnType.NULL, lambda _: None)

    def test_object(self):
        def value_factory(key):
            return Student(key, key)

        self._create_mapping_for_portable(666, 6, {"age": "BIGINT", "height": "REAL"})
        self._populate_map(value_factory=value_factory)
        result = self.execute('SELECT __key, this FROM "%s"' % self.map_name)
        self._validate_result(result, SqlColumnType.OBJECT, value_factory)

    def test_null_only_column(self):
        self._create_mapping("INTEGER")
        self._populate_map()
        result = self.execute(
            'SELECT __key, CAST(NULL AS INTEGER) as this FROM "%s"' % self.map_name
        )
        self._validate_result(result, SqlColumnType.INTEGER, lambda _: None)

    def _validate_rows(self, expected_type, value_factory=lambda key: key):
        result = self.execute('SELECT __key, this FROM "%s"' % self.map_name)
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
    <lite-member enabled="true" />%s
</hazelcast>
"""


@unittest.skipIf(
    compare_client_version("4.2") < 0 or compare_client_version("5.0") >= 0,
    "Tests the behaviour of the v4 client, with the features added in 4.2",
)
class SqlServiceV4LiteMemberClusterTest(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        return LITE_MEMBER_CONFIG % ""

    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    def setUp(self):
        skip_if_server_version_older_than(self, self.client, "4.2")
        skip_if_server_version_newer_than_or_equal(self, self.client, "5.0")

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


@unittest.skipIf(
    compare_client_version("5.0") < 0, "Tests the features added in 5.0 version of the client"
)
class SqlServiceV5LiteMemberClusterTest(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        is_v5_or_newer_server = compare_server_version_with_rc(cls.rc, "5.0") >= 0
        return LITE_MEMBER_CONFIG % (JET_ENABLED_CONFIG if is_v5_or_newer_server else "")

    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    def setUp(self):
        skip_if_server_version_older_than(self, self.client, "5.0")

    def test_execute(self):
        with self.assertRaises(HazelcastSqlError) as cm:
            with self.client.sql.execute("SOME QUERY").result() as result:
                result.update_count()

        # Make sure that exception is originating from the server
        self.assertEqual(self.member.uuid, str(cm.exception.originating_member_uuid))


@unittest.skipIf(
    compare_client_version("5.0") < 0, "Tests the features added in 5.0 version of the client"
)
class SqlServiceV5MixedClusterTest(HazelcastTestCase):

    rc = None
    cluster = None
    is_v5_or_newer_server = None
    client = None

    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.is_v5_or_newer_server = compare_server_version_with_rc(cls.rc, "5.0") >= 0

        cluster_config = (
            LITE_MEMBER_CONFIG % JET_ENABLED_CONFIG
            if cls.is_v5_or_newer_server
            else LITE_MEMBER_CONFIG % ""
        )
        cls.cluster = cls.create_cluster(cls.rc, cluster_config)
        cls.cluster.start_member()
        cls.cluster.start_member()

        script = """instance_0.getCluster().promoteLocalLiteMember();"""
        cls.rc.executeOnController(cls.cluster.id, script, Lang.JAVASCRIPT)

        cls.client = HazelcastClient(cluster_name=cls.cluster.id)

    @classmethod
    def tearDownClass(cls):
        cls.client.shutdown()
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    def setUp(self):
        skip_if_server_version_older_than(self, self.client, "5.0")

    def test_mixed_cluster(self):
        map_name = random_string()

        create_mapping_query = (
            """
        CREATE MAPPING %s (
            __key INT,
            this INT
        )
        TYPE IMaP 
        OPTIONS (
            'keyFormat' = 'int',
            'valueFormat' = 'int'
        )
        """
            % map_name
        )

        self.client.sql.execute(create_mapping_query).result()

        m = self.client.get_map(map_name).blocking()
        m.put(1, 1)
        with self.client.sql.execute("SELECT this FROM %s" % map_name).result() as result:
            rows = [row.get_object("this") for row in result]

        self.assertEqual(1, len(rows))
        self.assertEqual(1, rows[0])


@unittest.skipIf(
    compare_client_version("5.0") < 0, "Tests the features added in 5.0 version of the client"
)
class JetSqlTest(SqlTestBase):
    def _setup_skip_condition(self):
        skip_if_server_version_older_than(self, self.client, "5.0")

    def test_streaming_sql_query(self):
        with self.execute("SELECT * FROM TABLE(generate_stream(100))") as result:
            for idx, row in enumerate(result):
                self.assertEqual(idx, row.get_object("v"))
                if idx == 200:
                    break

    def test_federated_query(self):
        query = (
            """
        CREATE MAPPING "%s" (
            __key INT,
            name VARCHAR,
            age INT
        )
        TYPE IMap
        OPTIONS (
            'keyFormat' = 'int',
            'valueFormat' = 'json-flat'
        )
        """
            % self.map_name
        )

        self.execute(query)

        insert_into_query = (
            """
        INSERT INTO "%s" (__key, name, age) 
        VALUES (1, 'John', 42)
        """
            % self.map_name
        )

        with self.execute(insert_into_query) as result:
            self.assertEqual(0, self.update_count(result))

        self.assertEqual(1, self.map.size())
        entry = self.map.get(1)
        self.assertEqual({"name": "John", "age": 42}, entry.loads())


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
