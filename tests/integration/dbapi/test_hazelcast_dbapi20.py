from .dbapi20 import DatabaseAPI20Test
from hazelcast import db, HazelcastClient


class test_HazelcastDBAPI20(DatabaseAPI20Test):

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

    def setUp(self):
        client = HazelcastClient()
        booze = client.get_map(f"{test_HazelcastDBAPI20.table_prefix}booze").blocking()
        booze.destroy()
        barflys = client.get_map(f"{test_HazelcastDBAPI20.table_prefix}barflys").blocking()
        barflys.destroy()
        client.shutdown()
