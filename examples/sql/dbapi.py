import hazelcast

conn = hazelcast.connect()
cur = conn.cursor()
cur.execute(
    """
    CREATE OR REPLACE MAPPING stocks (
        __key INT,
        operation_date DATE,
        transfer VARCHAR,
        symbol VARCHAR,
        quantity DOUBLE,
        price DOUBLE
    )
    TYPE IMap
    OPTIONS (
      'keyFormat' = 'int',
      'valueFormat' = 'json-flat'
    )
    """
)

data = [
    (1, '2006-03-28', 'BUY', 'IBM', 1000, 45.0),
    (2, '2006-04-05', 'BUY', 'MSFT', 1000, 72.0),
    (3, '2006-04-06', 'SELL', 'IBM', 500, 53.0),
]
cur.executemany('SINK INTO stocks VALUES(?, CAST(? AS DATE), ?, ?, ?, ?)', data)

for row in cur.execute('SELECT * FROM stocks ORDER BY price'):
    print(row)

print(cur.description)

conn.close()