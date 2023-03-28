import threading

from hazelcast import HazelcastClient
from hazelcast.config import Config
from hazelcast.db import connect, Connection, Type, DatabaseError, InterfaceError
from hazelcast.sql import HazelcastSqlError
from tests.integration.backward_compatible.sql_test import (
    SqlTestBase,
    compare_server_version_with_rc,
    compare_client_version,
    SERVER_CONFIG,
    JET_ENABLED_CONFIG,
    Student,
)


class DbapiTestBase(SqlTestBase):

    rc = None
    cluster = None
    is_v5_or_newer_server = None
    is_v5_or_newer_client = None
    conn: Connection = None

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
        cfg = Config()
        cfg.cluster_name = cls.cluster.id
        cls.conn = connect(cfg)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        cls.client.shutdown()
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    def _create_mapping(self, value_format="INTEGER"):
        if not self.is_v5_or_newer_server:
            # Implicit mappings are removed in 5.0
            return
        q = f"""
        CREATE MAPPING "{self.map_name}" (
            __key INT,
            this {value_format}
        )
        TYPE IMaP 
        OPTIONS (
            'keyFormat' = 'int',
            'valueFormat' = '{value_format.lower()}'
        )
        """
        c = self.conn.cursor()
        c.execute(q)

    def _populate_map(self, entry_count=10, value_factory=lambda v: v):
        entries = [(i, value_factory(i)) for i in range(entry_count)]
        c = self.conn.cursor()
        c.executemany(f'INSERT INTO "{self.map_name}" VALUES(?, ?)', entries)


class DbapiTest(DbapiTestBase):
    def test_fetchone(self):
        self._create_mapping()
        entry_count = 11
        self._populate_map(entry_count)
        c = self.conn.cursor()
        c.execute(f'SELECT * FROM "{self.map_name}" where __key < ? order by __key', (5,))
        self.assertEqual(0, c.rownumber)
        row = c.fetchone()
        self.assertEqual((0, 0), (row.get_object("__key"), row.get_object("this")))
        self.assertEqual(1, c.rownumber)
        row = c.fetchone()
        self.assertEqual((1, 1), (row.get_object("__key"), row.get_object("this")))
        self.assertEqual(2, c.rownumber)

    def test_fetchmany(self):
        self._create_mapping()
        entry_count = 11
        self._populate_map(entry_count)
        c = self.conn.cursor()
        c.execute(f'SELECT * FROM "{self.map_name}" where __key < ? order by __key', (5,))
        self.assertEqual(0, c.rownumber)
        result = list(c.fetchmany(3))
        self.assertCountEqual(
            [(i, i) for i in range(3)],
            [(row.get_object("__key"), row.get_object("this")) for row in result],
        )
        self.assertEqual(3, c.rownumber)
        result = list(c.fetchmany(3))
        self.assertCountEqual(
            [(i, i) for i in range(3, 5)],
            [(row.get_object("__key"), row.get_object("this")) for row in result],
        )
        self.assertEqual(5, c.rownumber)

    def test_fetchall(self):
        self._create_mapping()
        entry_count = 11
        self._populate_map(entry_count)
        c = self.conn.cursor()
        c.execute(f'SELECT * FROM "{self.map_name}" where __key < ? order by __key', (5,))
        self.assertEqual(0, c.rownumber)
        result = list(c.fetchall())
        self.assertCountEqual(
            [(i, i) for i in range(5)],
            [(row.get_object("__key"), row.get_object("this")) for row in result],
        )
        self.assertEqual(5, c.rownumber)

    def test_cursor_iteration(self):
        self._create_mapping()
        entry_count = 5
        self._populate_map(entry_count)
        c = self.conn.cursor()
        c.execute(f'SELECT * FROM "{self.map_name}" order by __key LIMIT 3')
        items = []
        for row in c:
            items.append((row["__key"], row["this"]))
        target = [(0, 0), (1, 1), (2, 2)]
        self.assertEqual(target, items)

    def test_cursor_connection(self):
        c = self.conn.cursor()
        self.assertEqual(self.conn, c.connection)

    def test_description(self):
        self._create_mapping()
        self._populate_map(1)
        c = self.conn.cursor()
        c.execute(f'SELECT * FROM "{self.map_name}"')
        target = [
            ("__key", Type.INTEGER, None, None, None, None, True),
            ("this", Type.INTEGER, None, None, None, None, True),
        ]
        self.assertEqual(target, c.description)

    def test_connection_share(self):
        def f():
            c = self.conn.cursor()
            c.execute("show mappings;")
            c.fetchall()

        threads = []
        for i in range(100):
            t = threading.Thread(target=f)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        self.assertEqual(len(threads), len(self.conn._cursors))

    def test_execute_error(self):
        c = self.conn.cursor()
        with self.assertRaises(DatabaseError):
            c.execute(f'SELECT * FROM "{self.map_name}"')

    def test_executemany_error(self):
        c = self.conn.cursor()
        with self.assertRaises(DatabaseError):
            c.executemany(f"INVALID SQL", [("foo", 1)])

    def test_fetchone_error(self):
        self._create_mapping()
        self._populate_map(1)
        c = self.conn.cursor()
        c.execute(f'SELECT * FROM "{self.map_name}"')
        c._iter.it = MockIter(lambda: HazelcastSqlError("UUID", "CODE", "MSG", None))
        with self.assertRaises(DatabaseError):
            c.fetchone()

    def test_fetchmany_error(self):
        self._create_mapping()
        self._populate_map(1)
        c = self.conn.cursor()
        c.execute(f'SELECT * FROM "{self.map_name}"')
        c._iter.it = MockIter(lambda: HazelcastSqlError("UUID", "CODE", "MSG", None))
        with self.assertRaises(DatabaseError):
            c.fetchmany(2)


class MockIter:
    def __init__(self, fn):
        self.fn = fn

    def __iter__(self):
        return self

    def __next__(self):
        raise self.fn()
