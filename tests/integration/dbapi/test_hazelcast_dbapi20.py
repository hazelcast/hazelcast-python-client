from hazelcast.config import Config
from .dbapi20 import DatabaseAPI20Test
from hazelcast import db, HazelcastClient
from ..backward_compatible.sql_test import SqlTestBase


class test_HazelcastDBAPI20(SqlTestBase, DatabaseAPI20Test):

    rc = None
    cluster = None
    member = None
    driver = db
    connect_kw_args = {}
    table_prefix = "dbapi20test_"
    ddl1 = f'''
        CREATE OR REPLACE MAPPING {table_prefix}booze (
            name varchar external name "__key.name"
        ) TYPE IMAP OPTIONS (
            'keyFormat'='json-flat',
            'valueFormat'='json-flat'
        )    
    '''
    ddl2 = f'''
        CREATE OR REPLACE MAPPING {table_prefix}barflys (
            name varchar external name "__key.name",
            drink varchar external name "this.drink"
        ) TYPE IMAP OPTIONS (
            'keyFormat'='json-flat',
            'valueFormat'='json-flat'
        )    
    '''
    # ddl2 = 'create table %sbarflys (name varchar(20), drink varchar(30))' % table_prefix
    # xddl1 = 'drop table %sbooze' % table_prefix
    # xddl2 = 'drop table %sbarflys' % table_prefix

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cfg = Config()
        cfg.cluster_name = cls.cluster.id
        cls.connect_kw_args = {
            "config": cfg,
        }

    @classmethod
    def tearDownClass(cls):
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    def setUp(self):
        pass

    def tearDown(self):
        for name in ["booze", "barflys"]:
            m = self.client.get_map(f"{self.table_prefix}{name}").blocking()
            m.destroy()

    def test_nextset(self):
        # we don't support this.
        pass

    def test_setoutputsize(self):
        # we don't support this.
        pass
