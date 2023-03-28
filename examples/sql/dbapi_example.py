
from hazelcast.db import connect

# create a connection to the cluster
# this example assumes Hazelcast is running locally at the default host and port
conn = connect(host="localhost", port=5701)
# get a cursor
cur = conn.cursor()

# create a mapping
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

# add some data
data = [
    (1, "2006-03-28", "BUY", "IBM", 1000, 45.0),
    (2, "2006-04-05", "BUY", "MSFT", 1000, 72.0),
    (3, "2006-04-06", "SELL", "IBM", 500, 53.0),
]
cur.executemany("SINK INTO stocks VALUES(?, CAST(? AS DATE), ?, ?, ?, ?)", data)

# execute a SQL query and get rows
print("This is the first query...")
cur.execute("SELECT * FROM stocks ORDER BY price")
for row in cur.fetchmany(2):
    print(row["__key"], row["symbol"], row["quantity"], row["price"])
# close the cursors to release resources when the cursor
# will not be used anymore
cur.close()
# likewise, close the connection to release resources when the connection
# will not be used anymore
conn.close()

# this one uses a with statement, so the connection is automatically closed
# here's how you would use a DSN to specify connection parameters
with connect(dsn="hz://localhost:5701") as conn:
    print("Here's another query...")
    try:
        # the cursor is automatically closed
        with conn.cursor() as cur:
            cur.execute("SELECT symbol, quantity * price AS total_price FROM stocks ORDER BY price")
            for row in cur:
                print(row["symbol"], row["total_price"])
    except conn.Error as ex:
        print(ex)

