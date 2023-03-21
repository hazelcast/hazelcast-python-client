import hazelcast.db

conn = hazelcast.db.connect()
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
    (1, "2006-03-28", "BUY", "IBM", 1000, 45.0),
    (2, "2006-04-05", "BUY", "MSFT", 1000, 72.0),
    (3, "2006-04-06", "SELL", "IBM", 500, 53.0),
]
cur.executemany("SINK INTO stocks VALUES(?, CAST(? AS DATE), ?, ?, ?, ?)", data)

cur.execute("SELECT * FROM stocks ORDER BY price")

for row in cur.fetchmany(1):
    print(row["__key"], row["symbol"], row["quantity"], row["price"])

for row in cur.fetchmany(1):
    print(row["__key"], row["symbol"], row["quantity"], row["price"])

for row in cur.fetchmany(1):
    print(row["__key"], row["symbol"], row["quantity"], row["price"])

conn.close()

with hazelcast.db.connect() as conn:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM stocks ORDER BY price")
            for row in cur:
                print(row["symbol"], row["quantity"], row["price"])
    except conn.Error as ex:
        print(ex)
