import asyncio
import datetime
import decimal
import math
import types
import unittest
from unittest.mock import patch

from hazelcast.core import HazelcastJsonValue
from hazelcast.internal.asyncio_client import HazelcastClient
from hazelcast.sql import HazelcastSqlError, SqlExpectedResultType, SqlColumnType
from tests.hzrc.ttypes import Lang
from tests.integration.asyncio.base import SingleMemberTestCase, HazelcastTestCase
from tests.integration.backward_compatible.sql_test import Student, LITE_MEMBER_CONFIG
from tests.util import random_string


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


class SqlTestBase(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cluster_config = SERVER_CONFIG % JET_ENABLED_CONFIG
        cls.cluster = cls.create_cluster(cls.rc, cluster_config)
        cls.cluster.start_member()

    @classmethod
    def tearDownClass(cls):
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.client = await HazelcastClient.create_and_start(
            cluster_name=self.cluster.id, portable_factories={666: {6: Student}}
        )
        self.map_name = random_string()
        self.map = await self.client.get_map(self.map_name)

    async def asyncTearDown(self):
        await self.map.clear()
        await self.shutdown_all_clients()
        await super().asyncTearDown()

    async def _populate_map(self, entry_count=10, value_factory=lambda v: v):
        entries = {i: value_factory(i) for i in range(entry_count)}
        await self.map.put_all(entries)

    async def _create_mapping(self, value_format="INTEGER"):
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

        await self.execute(create_mapping_query)

    async def _create_mapping_for_portable(self, factory_id, class_id, columns):
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
        await self.execute(create_mapping_query)

    async def execute(self, query, *args):
        return await self.client.sql.execute(query, *args)

    async def execute_statement(self, query, *args, **kwargs):
        return await self.client.sql.execute(query, *args, **kwargs)

    def update_count(self, result):
        return result.update_count()

    def is_row_set(self, result):
        return result.is_row_set()

    def get_row_metadata(self, result):
        return result.get_row_metadata()


class SqlServiceTest(SqlTestBase):
    async def test_execute(self):
        await self._create_mapping()
        entry_count = 11
        await self._populate_map(entry_count)
        result = await self.execute('SELECT * FROM "%s"' % self.map_name)
        self.assertCountEqual(
            [(i, i) for i in range(entry_count)],
            [(row.get_object("__key"), row.get_object("this")) async for row in result],
        )

    async def test_execute_with_params(self):
        await self._create_mapping()
        entry_count = 13
        await self._populate_map(entry_count)
        result = await self.execute(
            'SELECT this FROM "%s" WHERE __key > ? AND this > ?' % self.map_name, 5, 6
        )
        self.assertCountEqual(
            [i for i in range(7, entry_count)],
            [row.get_object("this") async for row in result],
        )

    async def test_execute_with_mismatched_params_when_sql_has_more(self):
        await self._create_mapping()
        await self._populate_map()
        with self.assertRaises(HazelcastSqlError):
            result = await self.execute(
                'SELECT * FROM "%s" WHERE __key > ? AND this > ?' % self.map_name, 5
            )
            async for _ in result:
                pass

    async def test_execute_with_mismatched_params_when_params_has_more(self):
        await self._create_mapping()
        await self._populate_map()
        with self.assertRaises(HazelcastSqlError):
            result = await self.execute('SELECT * FROM "%s" WHERE this > ?' % self.map_name, 5, 6)
            async for _ in result:
                pass

    async def test_execute_statement(self):
        await self._create_mapping("VARCHAR")
        entry_count = 12
        await self._populate_map(entry_count, str)
        result = await self.execute_statement('SELECT this FROM "%s"' % self.map_name)
        self.assertCountEqual(
            [str(i) for i in range(entry_count)],
            [row.get_object_with_index(0) async for row in result],
        )

    async def test_execute_statement_with_params(self):
        await self._create_mapping_for_portable(666, 6, {"age": "BIGINT", "height": "REAL"})
        entry_count = 20
        await self._populate_map(entry_count, lambda v: Student(v, v))
        result = await self.execute_statement(
            'SELECT age FROM "%s" WHERE height = CAST(? AS REAL)' % self.map_name,
            13.0,
        )
        self.assertCountEqual([13], [row.get_object("age") async for row in result])

    async def test_execute_statement_with_mismatched_params_when_sql_has_more(self):
        await self._create_mapping()
        await self._populate_map()
        with self.assertRaises(HazelcastSqlError):
            result = await self.execute_statement(
                'SELECT * FROM "%s" WHERE __key > ? AND this > ?' % self.map_name,
                5,
            )
            async for _ in result:
                pass

    async def test_execute_statement_with_mismatched_params_when_params_has_more(self):
        await self._create_mapping()
        await self._populate_map()
        with self.assertRaises(HazelcastSqlError):
            result = await self.execute_statement(
                'SELECT * FROM "%s" WHERE this > ?' % self.map_name,
                5,
                6,
            )
            async for _ in result:
                pass

    async def test_execute_statement_with_timeout(self):
        await self._create_mapping_for_portable(666, 6, {"age": "BIGINT", "height": "REAL"})
        entry_count = 100
        await self._populate_map(entry_count, lambda v: Student(v, v))
        result = await self.execute_statement(
            'SELECT age FROM "%s" WHERE height < 10' % self.map_name,
            timeout=100,
        )
        self.assertCountEqual(
            [i for i in range(10)], [row.get_object("age") async for row in result]
        )

    async def test_execute_statement_with_cursor_buffer_size(self):
        await self._create_mapping_for_portable(666, 6, {"age": "BIGINT", "height": "REAL"})
        entry_count = 50
        await self._populate_map(entry_count, lambda v: Student(v, v))
        result = await self.execute_statement(
            'SELECT age FROM "%s"' % self.map_name,
            cursor_buffer_size=3,
        )
        with patch.object(result, "_fetch_next_page", wraps=result._fetch_next_page) as patched:
            self.assertCountEqual(
                [i for i in range(entry_count)], [row.get_object("age") async for row in result]
            )
            # -1 comes from the fact that, we don't fetch the first page.
            expected = math.ceil(entry_count / 3.0) - 1
            actual = patched.call_count
            self.assertEqual(expected, actual)

    # Can't test the case we would expect an update count, because the IMDG SQL
    # engine does not support such query as of now.
    async def test_execute_statement_with_expected_result_type_of_rows_when_rows_are_expected(self):
        await self._create_mapping_for_portable(666, 6, {"age": "BIGINT", "height": "REAL"})
        entry_count = 100
        await self._populate_map(entry_count, lambda v: Student(v, v))
        result = await self.execute_statement(
            'SELECT age FROM "%s" WHERE age < 3' % self.map_name,
            expected_result_type=SqlExpectedResultType.ROWS,
        )

        self.assertCountEqual(
            [i for i in range(3)], [row.get_object("age") async for row in result]
        )

    # Can't test the case we would expect an update count, because the IMDG SQL
    # engine does not support such query as of now.
    async def test_execute_statement_with_expected_result_type_of_update_count_when_rows_are_expected(
        self,
    ):
        await self._create_mapping()
        await self._populate_map()

        with self.assertRaises(HazelcastSqlError):
            result = await self.execute_statement(
                'SELECT * FROM "%s"' % self.map_name,
                expected_result_type=SqlExpectedResultType.UPDATE_COUNT,
            )
            async for _ in result:
                pass

    # Can't test the schema, because the IMDG SQL engine does not support
    # specifying a schema yet.
    async def test_provided_suggestions(self):
        # We don't create a mapping intentionally to get suggestions
        await self.map.put(1, "value-1")
        select_all_query = 'SELECT * FROM "%s"' % self.map_name
        with self.assertRaises(HazelcastSqlError) as cm:
            await self.execute(select_all_query)

        await self.execute(cm.exception.suggestion)
        async with await self.execute(select_all_query) as result:
            self.assertEqual(
                [(1, "value-1")],
                [(r.get_object("__key"), r.get_object("this")) async for r in result],
            )


class SqlResultTest(SqlTestBase):
    async def test_blocking_iterator(self):
        await self._create_mapping()
        await self._populate_map()
        result = await self.execute('SELECT __key FROM "%s"' % self.map_name)
        self.assertCountEqual(
            [i for i in range(10)], [row.get_object_with_index(0) async for row in result]
        )

    async def test_blocking_iterator_when_iterator_requested_more_than_once(self):
        await self._create_mapping()
        await self._populate_map()
        result = await self.execute('SELECT this FROM "%s"' % self.map_name)

        self.assertCountEqual(
            [i for i in range(10)], [row.get_object_with_index(0) async for row in result]
        )
        with self.assertRaises(ValueError):
            async for _ in result:
                pass

    async def test_blocking_iterator_with_multi_paged_result(self):
        await self._create_mapping()
        await self._populate_map()
        # Each page will contain just 1 result
        result = await self.execute_statement(
            'SELECT __key FROM "%s"' % self.map_name,
            cursor_buffer_size=1,
        )

        self.assertCountEqual(
            [i for i in range(10)], [row.get_object_with_index(0) async for row in result]
        )

    async def test_iterator(self):
        await self._create_mapping()
        await self._populate_map()
        result = await self.execute('SELECT __key FROM "%s"' % self.map_name)
        iterator = result.iterator()
        rows = []
        async for row in iterator:
            rows.append(row.get_object_with_index(0))

        def assertion():
            self.assertCountEqual(
                [i for i in range(10)],
                rows,
            )

        await self.assertTrueEventually(assertion)

    async def test_iterator_when_iterator_requested_more_than_once(self):
        await self._create_mapping()
        await self._populate_map()
        result = await self.execute('SELECT this FROM "%s"' % self.map_name)
        iterator = result.iterator()
        rows = []
        async for row in iterator:
            rows.append(row.get_object("this"))

        self.assertCountEqual([i for i in range(10)], rows)
        with self.assertRaises(ValueError):
            result.iterator()

    async def test_iterator_with_multi_paged_result(self):
        await self._create_mapping()
        await self._populate_map()
        # Each page will contain just 1 result
        result = await self.execute_statement(
            'SELECT __key FROM "%s"' % self.map_name,
            cursor_buffer_size=1,
        )
        iterator = result.iterator()
        rows = []
        async for row in iterator:
            rows.append(row.get_object_with_index(0))

        self.assertCountEqual([i for i in range(10)], rows)

    async def test_request_blocking_iterator_after_iterator(self):
        await self._create_mapping()
        await self._populate_map()
        result = await self.execute('SELECT * FROM "%s"' % self.map_name)
        _ = result.iterator()
        with self.assertRaises(ValueError):
            async for _ in result:
                pass

    async def test_request_iterator_after_blocking_iterator(self):
        await self._create_mapping()
        await self._populate_map()
        result = await self.execute('SELECT * FROM "%s"' % self.map_name)
        async for _ in result:
            pass

        with self.assertRaises(ValueError):
            result.iterator()

    # Can't test the case we would expect row to be not set, because the IMDG SQL
    # engine does not support update/insert queries now.
    async def test_is_row_set_when_row_is_set(self):
        await self._create_mapping()
        await self._populate_map()
        result = await self.execute('SELECT * FROM "%s"' % self.map_name)
        self.assertTrue(self.is_row_set(result))

    # Can't test the case we would expect a non-negative updated count, because the IMDG SQL
    # engine does not support update/insert queries now.
    async def test_update_count_when_there_is_no_update(self):
        await self._create_mapping()
        await self._populate_map()
        result = await self.execute('SELECT * FROM "%s" WHERE __key > 5' % self.map_name)
        self.assertEqual(-1, self.update_count(result))

    async def test_get_row_metadata(self):
        await self._create_mapping("VARCHAR")
        await self._populate_map(value_factory=str)
        result = await self.execute('SELECT __key, this FROM "%s"' % self.map_name)
        row_metadata = self.get_row_metadata(result)
        self.assertEqual(2, row_metadata.column_count)
        columns = row_metadata.columns
        self.assertEqual(SqlColumnType.INTEGER, columns[0].type)
        self.assertEqual(SqlColumnType.VARCHAR, columns[1].type)
        self.assertTrue(columns[0].nullable)
        self.assertTrue(columns[1].nullable)

    async def test_close_after_query_execution(self):
        await self._create_mapping()
        await self._populate_map()
        result = await self.execute('SELECT * FROM "%s"' % self.map_name)
        async for _ in result:
            pass

        self.assertIsNone(await result.close())

    async def test_close_when_query_is_active(self):
        await self._create_mapping()
        await self._populate_map()
        # Each page will contain 1 row
        result = await self.execute_statement(
            'SELECT * FROM "%s"' % self.map_name,
            cursor_buffer_size=1,
        )
        # Fetch couple of pages
        iterator = result.iterator()
        await iterator.__anext__()
        self.assertIsNone(await result.close())
        with self.assertRaises(HazelcastSqlError):
            # Next fetch requests should fail
            await iterator.__anext__()

    async def test_with_statement(self):
        await self._create_mapping()
        await self._populate_map()
        async with await self.execute('SELECT this FROM "%s"' % self.map_name) as result:
            self.assertCountEqual(
                [i for i in range(10)], [row.get_object_with_index(0) async for row in result]
            )

    async def test_with_statement_when_iteration_throws(self):
        await self._create_mapping()
        await self._populate_map()
        with self.assertRaises(RuntimeError):
            async with await self.execute_statement(
                'SELECT this FROM "%s"' % self.map_name,
                cursor_buffer_size=1,  # so that it doesn't close immediately
            ) as result:
                async for _ in result:
                    raise RuntimeError("expected")

        res = await result.close()
        self.assertEqual(res, None)

    async def test_deserialization_error(self):
        # Using a Portable that is not defined on the client-side.
        await self._create_mapping_for_portable(666, 1, {})

        script = (
            """
        var m = instance_0.getMap("%s");
        m.put(1, new com.hazelcast.client.test.Employee(1, "Joe"));
        """
            % self.map_name
        )
        res = await asyncio.get_running_loop().run_in_executor(
            None, self.rc.executeOnController, self.cluster.id, script, Lang.JAVASCRIPT
        )
        self.assertTrue(res.success)
        with self.assertRaisesRegex(HazelcastSqlError, "Failed to deserialize query result value"):
            await self.execute('SELECT __key, this FROM "%s"' % self.map_name)

    async def test_rows_as_dict_or_list(self):
        await self._create_mapping("VARCHAR")
        entry_count = 20

        def value_factory(v):
            return "value-%s" % v

        await self._populate_map(entry_count, value_factory)
        expected = [(i, value_factory(i)) for i in range(entry_count)]
        async with await self.execute('SELECT __key, this FROM "%s"' % self.map_name) as result:
            # Verify that both row[integer] and row[string] works
            self.assertCountEqual(expected, [(row[0], row["this"]) async for row in result])


class SqlColumnTypesReadTest(SqlTestBase):
    async def test_varchar(self):
        def value_factory(key):
            return "val-%s" % key

        await self._create_mapping("VARCHAR")
        await self._populate_map(value_factory=value_factory)
        await self._validate_rows(SqlColumnType.VARCHAR, value_factory)

    async def test_boolean(self):
        def value_factory(key):
            return key % 2 == 0

        await self._create_mapping("BOOLEAN")
        await self._populate_map(value_factory=value_factory)
        await self._validate_rows(SqlColumnType.BOOLEAN, value_factory)

    async def _validate_rows(self, expected_type, value_factory=lambda key: key):
        result = await self.execute('SELECT __key, this FROM "%s"' % self.map_name)
        await self._validate_result(result, expected_type, value_factory)

    async def test_tiny_int(self):
        await self._create_mapping("TINYINT")
        await self._populate_map_via_rc("new java.lang.Byte(key)")
        await self._validate_rows(SqlColumnType.TINYINT)

    async def test_small_int(self):
        await self._create_mapping("SMALLINT")
        await self._populate_map_via_rc("new java.lang.Short(key)")
        await self._validate_rows(SqlColumnType.SMALLINT)

    async def test_integer(self):
        await self._create_mapping("INTEGER")
        await self._populate_map_via_rc("new java.lang.Integer(key)")
        await self._validate_rows(SqlColumnType.INTEGER)

    async def test_big_int(self):
        await self._create_mapping("BIGINT")
        await self._populate_map_via_rc("new java.lang.Long(key)")
        await self._validate_rows(SqlColumnType.BIGINT)

    async def test_real(self):
        await self._create_mapping("REAL")
        await self._populate_map_via_rc("new java.lang.Float(key * 1.0 / 8)")
        await self._validate_rows(SqlColumnType.REAL, lambda x: x * 1.0 / 8)

    async def test_double(self):
        await self._create_mapping("DOUBLE")
        await self._populate_map_via_rc("new java.lang.Double(key * 1.0 / 1.1)")
        await self._validate_rows(SqlColumnType.DOUBLE, lambda x: x * 1.0 / 1.1)

    async def test_date(self):
        def value_factory(key):
            return datetime.date(key + 2000, key + 1, key + 1)

        await self._create_mapping("DATE")
        await self._populate_map_via_rc("java.time.LocalDate.of(key + 2000, key + 1, key + 1)")
        await self._validate_rows(SqlColumnType.DATE, value_factory)

    async def test_time(self):
        def value_factory(key):
            return datetime.time(key, key, key, key)

        await self._create_mapping("TIME")
        await self._populate_map_via_rc("java.time.LocalTime.of(key, key, key, key * 1000)")
        await self._validate_rows(SqlColumnType.TIME, value_factory)

    async def test_timestamp(self):
        def value_factory(key):
            return datetime.datetime(key + 2000, key + 1, key + 1, key, key, key, key)

        await self._create_mapping("TIMESTAMP")
        await self._populate_map_via_rc(
            "java.time.LocalDateTime.of(key + 2000, key + 1, key + 1, key, key, key, key * 1000)"
        )
        await self._validate_rows(SqlColumnType.TIMESTAMP, value_factory)

    async def test_timestamp_with_time_zone(self):
        def value_factory(key):
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

        await self._create_mapping("TIMESTAMP WITH TIME ZONE")
        await self._populate_map_via_rc(
            "java.time.OffsetDateTime.of(key + 2000, key + 1, key + 1, key, key, key, key * 1000, "
            "java.time.ZoneOffset.ofHours(key))"
        )
        await self._validate_rows(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, value_factory)

    async def test_decimal(self):
        def value_factory(key):
            return decimal.Decimal((0, (key,), -1 * key))

        await self._create_mapping("DECIMAL")
        await self._populate_map_via_rc("java.math.BigDecimal.valueOf(key, key)")
        await self._validate_rows(SqlColumnType.DECIMAL, value_factory)

    async def test_null(self):
        await self._create_mapping("INTEGER")
        await self._populate_map()
        result = await self.execute('SELECT __key, NULL AS this FROM "%s"' % self.map_name)
        await self._validate_result(result, SqlColumnType.NULL, lambda _: None)

    async def test_object(self):
        def value_factory(key):
            return Student(key, key)

        await self._create_mapping_for_portable(666, 6, {"age": "BIGINT", "height": "REAL"})
        await self._populate_map(value_factory=value_factory)
        result = await self.execute('SELECT __key, this FROM "%s"' % self.map_name)
        await self._validate_result(result, SqlColumnType.OBJECT, value_factory)

    async def test_null_only_column(self):
        await self._create_mapping("INTEGER")
        await self._populate_map()
        result = await self.execute(
            'SELECT __key, CAST(NULL AS INTEGER) as this FROM "%s"' % self.map_name
        )
        await self._validate_result(result, SqlColumnType.INTEGER, lambda _: None)

    async def test_json(self):
        def value_factory(key):
            return HazelcastJsonValue({"key": key})

        await self._create_mapping("JSON")
        await self._populate_map(value_factory=value_factory)
        result = await self.execute(f'SELECT __key, this FROM "{self.map_name}"')
        await self._validate_result(result, SqlColumnType.JSON, value_factory)

    async def _validate_result(self, result, expected_type, factory):
        async for row in result:
            key = row.get_object("__key")
            expected_value = factory(key)
            row_metadata = row.metadata
            self.assertEqual(2, row_metadata.column_count)
            column_metadata = row_metadata.get_column(1)
            self.assertEqual(expected_type, column_metadata.type)
            self.assertEqual(expected_value, row.get_object("this"))

    async def _populate_map_via_rc(self, new_object_literal):
        script = """
        var map = instance_0.getMap("%s");
        for (var key = 0; key < 10; key++) {
            map.set(new java.lang.Integer(key), %s);
        }
        """ % (
            self.map_name,
            new_object_literal,
        )
        response = await asyncio.get_running_loop().run_in_executor(
            None, self.rc.executeOnController, self.cluster.id, script, Lang.JAVASCRIPT
        )
        self.assertTrue(response.success)


class SqlServiceV5LiteMemberClusterTest(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        return LITE_MEMBER_CONFIG % JET_ENABLED_CONFIG

    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    async def test_execute(self):
        with self.assertRaises(HazelcastSqlError) as cm:
            async with await self.client.sql.execute("SOME QUERY") as result:
                result.update_count()

        # Make sure that exception is originating from the server
        self.assertEqual(self.member.uuid, str(cm.exception.originating_member_uuid))


class SqlServiceV5MixedClusterTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cluster_config = SERVER_CONFIG % JET_ENABLED_CONFIG
        cls.cluster = cls.create_cluster(cls.rc, cluster_config)
        cls.cluster.start_member()
        cls.cluster.start_member()
        script = """instance_0.getCluster().promoteLocalLiteMember();"""
        cls.rc.executeOnController(cls.cluster.id, script, Lang.JAVASCRIPT)

    @classmethod
    def tearDownClass(cls):
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.client = await HazelcastClient.create_and_start(
            cluster_name=self.cluster.id, portable_factories={666: {6: Student}}
        )
        self.map_name = random_string()
        self.map = await self.client.get_map(self.map_name)

    async def asyncTearDown(self):
        await self.map.clear()
        await self.shutdown_all_clients()
        await super().asyncTearDown()

    async def test_mixed_cluster(self):
        map_name = random_string()

        create_mapping_query = (
            """
        CREATE MAPPING "%s" (
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

        await self.client.sql.execute(create_mapping_query)
        m = await self.client.get_map(map_name)
        await m.put(1, 1)
        async with await self.client.sql.execute('SELECT this FROM "%s"' % map_name) as result:
            rows = [row.get_object("this") async for row in result]

        self.assertEqual(1, len(rows))
        self.assertEqual(1, rows[0])


class JetSqlTest(SqlTestBase):
    async def test_streaming_sql_query(self):
        async with await self.execute("SELECT * FROM TABLE(generate_stream(100))") as result:
            idx = 0
            async for row in result:
                self.assertEqual(idx, row.get_object("v"))
                if idx == 200:
                    break
                idx += 1

    async def test_federated_query(self):
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

        await self.execute(query)
        insert_into_query = (
            """
        INSERT INTO "%s" (__key, name, age) 
        VALUES (1, 'John', 42)
        """
            % self.map_name
        )

        async with await self.execute(insert_into_query) as result:
            self.assertEqual(0, self.update_count(result))

        self.assertEqual(1, await self.map.size())
        entry = await self.map.get(1)
        self.assertEqual({"name": "John", "age": 42}, entry.loads())
