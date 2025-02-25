SQL
===

This chapter provides information on how you can run SQL queries
on a Hazelcast cluster using the Python client.

Hazelcast API
-------------

You can use SQL to query data in maps, Kafka topics, or a variety of file
systems. Results can be sent directly to the client or inserted into maps or
Kafka topics. For streaming queries, you can submit them to a cluster as jobs
to run in the background.

.. warning::

    The SQL feature is stabilized in 5.0 versions of the client and the
    Hazelcast platform. In order for the client and the server to be fully
    compatible with each other, their major versions must be the same.

.. note::

    In order to use SQL service from the Python client, Jet engine must be
    enabled on the members and the ``hazelcast-sql`` module must be in the
    classpath of the members.

    If you are using the CLI, Docker image, or distributions to start Hazelcast
    members, then you don't need to do anything, as the above preconditions are
    already satisfied for such members.

    However, if you are using Hazelcast members in the embedded mode, or
    receiving errors saying that ``The Jet engine is disabled`` or ``Cannot
    execute SQL query because "hazelcast-sql" module is not in the classpath.``
    while executing queries, enable the Jet engine following one of the
    instructions pointed out in the error message, or add the ``hazelcast-sql``
    module to your member's classpath.

Supported Queries
~~~~~~~~~~~~~~~~~

**Ad-Hoc Queries**

Query large datasets either in one or multiple systems and/or run aggregations
on them to get deeper insights.

See the `Get Started with SQL Over Maps
<https://docs.hazelcast.com/hazelcast/latest/sql/get-started-sql.html>`__ tutorial
for reference.

**Streaming Queries**

Also known as continuous queries, these keep an open connection to a streaming
data source and run a continuous query to get near real-time updates.

See the `Get Started with SQL Over Kafka
<https://docs.hazelcast.com/hazelcast/latest/sql/learn-sql.html>`__ tutorial
for reference.

**Federated Queries**

Query different datasets such as Kafka topics and Hazelcast maps, using a
single query. Normally, querying in SQL is database or dataset-specific.
However, with :ref:`mappings`, you can pull information
from different sources to present a more complete picture.

See the `Get Started with SQL Over Files
<https://docs.hazelcast.com/hazelcast/latest/sql/get-started-sql-files.html>`__
tutorial for reference.

.. _mappings:

Mappings
~~~~~~~~

To connect to data sources and query them as if they were tables, the SQL
service uses a concept called *mappings*.

Mappings store essential metadata about the source’s data model, data access
patterns, and serialization formats so that the SQL service can connect to the
data source and query it.

You can create mappings for the following data sources by using the
`CREATE MAPPING
<https://docs.hazelcast.com/hazelcast/latest/sql/create-mapping.html>`__
statement:

- `Hazelcast maps
  <https://docs.hazelcast.com/hazelcast/latest/sql/mapping-to-maps.html>`__
- `Kafka topics
  <https://docs.hazelcast.com/hazelcast/latest/sql/mapping-to-kafka.html>`__
- `File systems
  <https://docs.hazelcast.com/hazelcast/latest/sql/mapping-to-a-file-system.html>`__

Querying Map
~~~~~~~~~~~~

With SQL you can query the keys and values of maps in your cluster.

Assume that we have a map called ``employees`` that contains values of type
``Employee``:

.. code:: python

    class Employee(Portable):
        def __init__(self, name=None, age=None):
            self.name = name
            self.age = age

        def write_portable(self, writer):
            writer.write_string("name", self.name)
            writer.write_int("age", self.age)

        def read_portable(self, reader):
            self.name = reader.read_string("name")
            self.age = reader.read_int("age")

        def get_factory_id(self):
            return 1

        def get_class_id(self):
            return 2

    employees = client.get_map("employees").blocking()

    employees.set(1, Employee("John Doe", 33))
    employees.set(2, Employee("Jane Doe", 29))

Before starting to query data, we must create a *mapping* for the ``employees``
map. The details of ``CREATE MAPPING`` statement is discussed in the
`reference manual
<https://docs.hazelcast.com/hazelcast/latest/sql/mapping-to-maps.html>`__. For
the ``Employee`` class above, the mapping statement is shown below. It is
enough to create the mapping once per map.

.. code:: python

    client.sql.execute(
        """
    CREATE MAPPING employees (
        __key INT,
        name VARCHAR,
        age INT
    )
    TYPE IMap
    OPTIONS (
      'keyFormat' = 'int',
      'valueFormat' = 'portable',
      'valuePortableFactoryId' = '1',
      'valuePortableClassId' = '2'
    )
        """
    ).result()

The following code prints names of the employees whose age is less than ``30``:

.. code:: python

    result = client.sql.execute("SELECT name FROM employees WHERE age < 30").result()

    for row in result:
        name = row["name"]
        print(name)

The following subsections describe how you can access Hazelcast maps
and perform queries on them in more details.

**Case Sensitivity**

Mapping names and field names are case-sensitive.

For example, you can access an ``employees`` map as ``employees`` but not as
``Employees``.

**Key and Value Objects**

A map entry consists of a key and a value. These are accessible through
the ``__key`` and ``this`` aliases. The following query returns the keys and
values of all entries in the map:

.. code:: sql

    SELECT __key, this FROM employees

**"SELECT *" Queries**

You may use the ``SELECT * FROM <table>`` syntax to get all the table fields.

The ``__key`` and ``this`` fields are returned by the ``SELECT *`` queries if
they do not have nested fields. For the ``employees`` map, the following query
does not return the ``this`` field, because the value has nested fields
``name`` and ``age``:

.. code:: sql

    -- Returns __key, name, age
    SELECT * FROM employee

**Key and Value Fields**

You may also access the nested fields of a key or a value. The list of exposed
fields depends on the serialization format, as described `Querying Maps with
SQL <https://docs.hazelcast.com/hazelcast/latest/sql/querying-maps-sql.html>`__
section.

**Using Query Parameters**

You can use query parameters to build safer and faster SQL queries.

A query parameter is a piece of information that you supply to a query before
you run it. Parameters can be used by themselves or as part of a larger
expression to form a criterion in the query.

.. code:: python

    age_to_compare = 30
    client.sql.execute("SELECT * FROM employees WHERE age > ?", age_to_compare).result()

Instead of putting data straight into an SQL statement, you use the ``?``
placeholder in your client code to indicate that you will replace that
placeholder with a parameter.

Query parameters have the following benefits:

- Faster execution of similar queries. If you submit more than one query where
  only a value changes, the SQL service uses the cached query plan from the
  first query rather than optimizing each query again.
- Protection against SQL injection. If you use query parameters, you don’t need
  to escape special characters in user-provided strings.

Querying JSON Objects
~~~~~~~~~~~~~~~~~~~~~

In Hazelcast, the SQL service supports the following ways of working with
JSON data:

- ``json``: Maps JSON data to a single column of ``JSON`` type where you can
  use `JsonPath
  <https://docs.hazelcast.com/hazelcast/latest/sql/working-with-json#querying-json>`__
  syntax to query and filter it, including nested levels.
- ``json-flat``: Maps JSON top-level fields to columns with non-JSON types
  where you can query only top-level keys.

**json**

To query ``json`` objects, you should create an explicit mapping using the
`CREATE MAPPING
<https://docs.hazelcast.com/hazelcast/latest/sql/create-mapping.html>`__
statement, similar to the example above.

For example, this code snippet creates a mapping to a new map called
``json_employees``, which stores the JSON values as ``HazelcastJsonValue``
objects and queries it using nested fields, which is not possible with the
``json-flat`` type:

.. code:: python

    client.sql.execute(
        """
    CREATE OR REPLACE MAPPING json_employees
    TYPE IMap
    OPTIONS (
        'keyFormat' = 'int',
        'valueFormat' = 'json'
    )
        """
    ).result()

    json_employees = client.get_map("json_employees").blocking()

    json_employees.set(
        1,
        HazelcastJsonValue(
            {
                "personal": {"name": "John Doe"},
                "job": {"salary": 60000},
            }
        ),
    )

    json_employees.set(
        2,
        HazelcastJsonValue(
            {
                "personal": {"name": "Jane Doe"},
                "job": {"salary": 80000},
            }
        ),
    )

    with client.sql.execute(
        """
    SELECT JSON_VALUE(this, '$.personal.name') AS name
    FROM json_employees
    WHERE JSON_VALUE(this, '$.job.salary' RETURNING INT) > ?
        """,
        75000,
    ).result() as result:
        for row in result:
            print(f"Name: {row['name']}")

The ``json`` data type comes with full support for querying JSON in maps and
Kafka topics.


**JSON Functions**

Hazelcast supports the following functions, which can retrieve JSON data.

- `JSON_QUERY <https://docs.hazelcast.com/hazelcast/latest/sql/functions-and-operators#json_query>`__
  : Extracts a JSON value from a JSON document or a JSON-formatted string that
  matches a given JsonPath expression.

- `JSON_VALUE <https://docs.hazelcast.com/hazelcast/latest/sql/functions-and-operators#json_value>`__
  : Extracts a primitive value, such as a string, number, or boolean that
  matches a given JsonPath expression. This function returns ``NULL`` if a
  non-primitive value is matched, unless the ``ON ERROR`` behavior is changed.

- `JSON_ARRAY <https://docs.hazelcast.com/hazelcast/latest/sql/functions-and-operators#json_array>`__
  : Returns a JSON array from a list of input data.

- `JSON_OBJECT <https://docs.hazelcast.com/hazelcast/latest/sql/functions-and-operators#json_object>`__
  : Returns a JSON object from the given key/value pairs.

**json-flat**

To query ``json-flat`` objects, you should create an explicit mapping using the
`CREATE MAPPING
<https://docs.hazelcast.com/hazelcast/latest/sql/create-mapping.html>`__
statement, similar to the example above.

For example, this code snippet creates a mapping to a new map called
``json_flat_employees``, which stores the JSON values with columns ``name``
and ``salary`` as ``HazelcastJsonValue`` objects and queries it using
top-level fields:

.. code:: python

    client.sql.execute(
        """
    CREATE OR REPLACE MAPPING json_flat_employees (
        __key INT,
        name VARCHAR,
        salary INT
    )
    TYPE IMap
    OPTIONS (
        'keyFormat' = 'int',
        'valueFormat' = 'json-flat'
    )
        """
    ).result()

    json_flat_employees = client.get_map("json_flat_employees").blocking()

    json_flat_employees.set(
        1,
        HazelcastJsonValue(
            {
                "name": "John Doe",
                "salary": 60000,
            }
        ),
    )

    json_flat_employees.set(
        2,
        HazelcastJsonValue(
            {
                "name": "Jane Doe",
                "salary": 80000,
            }
        ),
    )

    with client.sql.execute(
            """
    SELECT name
    FROM json_flat_employees
    WHERE salary > ?
            """,
            75000,
    ).result() as result:
        for row in result:
            print(f"Name: {row['name']}")

Note that, in ``json-flat`` type, top-level columns must be explicitly
specified while creating the mapping.

The ``json-flat`` format comes with partial support for querying JSON in maps,
Kafka topics, and files.

For more information about working with JSON using SQL see
`Working with JSON
<https://docs.hazelcast.com/hazelcast/latest/sql/working-with-json>`__
in Hazelcast reference manual.


SQL Statements
~~~~~~~~~~~~~~

**Data Manipulation Language(DML) Statements**

- `SELECT <https://docs.hazelcast.com/hazelcast/latest/sql/select.html>`__:
  Read data from a table.
- `SINK INTO/INSERT INTO
  <https://docs.hazelcast.com/hazelcast/latest/sql/sink-into.html>`__:
  Ingest data into a map and/or forward data to other systems.
- `UPDATE <https://docs.hazelcast.com/hazelcast/latest/sql/update.html>`__:
  Overwrite values in map entries.
- `DELETE <https://docs.hazelcast.com/hazelcast/latest/sql/delete.html>`__:
  Delete map entries.

**Data Definition Language(DDL) Statements**

- `CREATE MAPPING
  <https://docs.hazelcast.com/hazelcast/latest/sql/create-mapping.html>`__:
  Map a local or remote data object to a table that Hazelcast can access.
- `SHOW MAPPINGS
  <https://docs.hazelcast.com/hazelcast/latest/sql/show-mappings.html>`__:
  Get the names of existing mappings.
- `DROP MAPPING
  <https://docs.hazelcast.com/hazelcast/latest/sql/drop-mapping.html>`__:
  Remove a mapping.

**Job Management Statements**

- `CREATE JOB
  <https://docs.hazelcast.com/hazelcast/latest/sql/create-job.html>`__:
  Create a job that is not tied to the client session.
- `ALTER JOB
  <https://docs.hazelcast.com/hazelcast/latest/sql/alter-job.html>`__:
  Restart, suspend, or resume a job.
- `SHOW JOBS
  <https://docs.hazelcast.com/hazelcast/latest/sql/show-jobs.html>`__:
  Get the names of all running jobs.
- `DROP JOB <https://docs.hazelcast.com/hazelcast/latest/sql/drop-job.html>`__:
  Cancel a job.
- `CREATE OR REPLACE SNAPSHOT (Enterprise only)
  <https://docs.hazelcast.com/hazelcast/latest/sql/create-snapshot.html>`__:
  Create a snapshot of a running job, so you can stop and restart it at a
  later date.
- `DROP SNAPSHOT (Enterprise only)
  <https://docs.hazelcast.com/hazelcast/latest/sql/drop-snapshot.html>`__:
  Cancel a running job.

Data Types
~~~~~~~~~~

The SQL service supports a set of SQL data types. Every data type is mapped to
a Python type that represents the type’s value.

======================== ========================================
Type Name                Python Type
======================== ========================================
BOOLEAN                  bool
VARCHAR                  str
TINYINT                  int
SMALLINT                 int
INTEGER                  int
BIGINT                   int
DECIMAL                  decimal.Decimal
REAL                     float
DOUBLE                   float
DATE                     datetime.date
TIME                     datetime.time
TIMESTAMP                datetime.datetime
TIMESTAMP_WITH_TIME_ZONE datetime.datetime (with non-None tzinfo)
OBJECT                   Any Python type
JSON                     HazelcastJsonValue
======================== ========================================

Functions and Operators
~~~~~~~~~~~~~~~~~~~~~~~

Hazelcast supports logical and ``IS`` predicates, comparison and mathematical
operators, and aggregate, mathematical, trigonometric, string, table-valued,
and special functions.

See the `Reference Manual
<https://docs.hazelcast.com/hazelcast/latest/sql/expressions.html>`__
for details.

Improving the Performance of SQL Queries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can improve the performance of queries over maps by indexing map entries.

To find out more about indexing map entries, see
:func:`add_index() <hazelcast.proxy.map.Map.add_index>` method.

If you find that your queries lead to out of memory exceptions (OOME), consider
decreasing the value of the Jet engine’s `max-processor-accumulated-records
option
<https://docs.hazelcast.com/hazelcast/latest/configuration/jet-configuration#list-of-configuration-options>`__.

Limitations
~~~~~~~~~~~

SQL has the following limitations. We plan to remove these limitations in
future releases.

- You cannot run SQL queries on lite members.
- The only supported Hazelcast data structure is map. You cannot query other
  data structures such as replicated maps.
- Limited support for joins. See `Join Tables
  <https://docs.hazelcast.com/hazelcast/latest/sql/select.html#join-tables>`__.

DBAPI-2 Interface
-----------------

`hazelcast.db` module supports the Python standard `DBAPI-2 Specification <https://peps.python.org/pep-0249/>`__.

Connection
~~~~~~~~~~

The :py:func:`connect() <hazelcast.db.connect>` function creates a connection to the cluster
and returns a :class:`Connection <hazelcast.db.Connection>` object.

.. code:: python

    from hazelcast.db import connect
    conn = connect()

The :py:func:`connect() <hazelcast.db.connect>` function connects to the default cluster by default.
There are a few ways to pass the connection parameters.

You can use the following keyword arguments:

- ``host``: Host part of the cluster address, by default: ``localhost``.
- ``port``: Port part of the cluster address, by default: ``5701``.
- ``cluster_name``: Cluster name, by default: ``dev``.
- ``user``: Username for the cluster. Requires Hazelcast EE.
- ``password``: Password for the cluster. Requires Hazelcast EE.

.. code:: python

    from hazelcast.db import connect
    conn = connect(user="localhost", port=5701)

You can also provide a DSN (Data Source Name) string to configure the connection.
The format of the DSN is ``hz://[user:password]@address1:port1[,address2:port2, ...][?option1=value1[&option2=value2 ...]]``
The following options are supported:

- ``cluster.name``: Hazelcast cluster name.
- ``cloud.token``: Cloud discovery token.
- ``smart``: Enables smart routing when true. Defaults to Python client default.
- ``ssl``: Enables SSL for client connection.
- ``ssl.ca.path``: Path to the CA file.
- ``ssl.cert.path``: Path to the certificate file.
- ``ssl.key.path``: Path to the private key file.
- ``ssl.key.password``: Password to the key file.

.. code:: python

    from hazelcast.db import connect
    conn = connect(dsn="hz://admin:ssap@demo.hazelcast.com?cluster.name=demo1")

In case you have to pass some options which are not supported by the methods above,
you can also pass a :class:`hazelcast.config.Config` object as the first argument to ``connect``.

.. code:: python

    from hazelcast.db import connect
    from hazelcast.config import Config
    config = Config()
    config.compact_serializers = [AddressSerializer()]
    conn = connect(config)

Once the connection is created, you can create a :class:`hazelcast.db.Cursor` object from it
to execute queries. This is explained in the next section.
Finally, you can close the ``Connection`` object to release its resources if you are done with it.

.. code:: python

    conn.close()

You can use a ``with`` statement to automatically close a ``Connection``.

.. code:: python

    from hazelcast.db import connect
    with connect() as conn:
        # use conn in this block
    # conn is automatically closed here

Cursors
~~~~~~~

The first step of executing a query is, getting a :class:`hazelcast.db.Cursor` from the connection.

.. code:: python

    cursor = conn.cursor()

Then, you can execute a SQL query using the :meth:`hazelcast.db.Cursor.execute` method.
You can use this method to run all kinds of queries.

.. code:: python

    cursor.execute("SELECT * FROM stocks ORDER BY price")

Use the question mark (``?``) as a placeholder if you are passing arguments
in the query. The actual arguments should be passed in a tuple.

.. code:: python

    cursor.execute("SELECT * FROM stocks WHERE price > ? ORDER BY price", (50,))

:meth:`hazelcast.db.Cursor.executemany` is also available, which enables running the same query
with different kinds of value sets. This method should only be used my mutating queries, such as ``INSERT``.

.. code:: python

    data = [
        (1, "2006-03-28", "BUY", "IBM", 1000, 45.0),
        (2, "2006-04-05", "BUY", "MSFT", 1000, 72.0),
        (3, "2006-04-06", "SELL", "IBM", 500, 53.0),
    ]
    cursor.executemany("INSERT INTO stocks VALUES(?, CAST(? AS DATE), ?, ?, ?, ?)", data)

**Mutating Queries**

Mutating queries such as ``UPDATE``, ``DELETE`` and ``INSERT`` updates, deletes
data or adds new rows. You can use ``execue`` or ``executemany`` for those
queries.

.. code:: python

    cursor.execute("INSERT INTO stocks(__key, price) VALUES(10, 40)")


**Row Returning Queries**

Queries such as ``SELECT`` and ``SHOW`` return rows. Once you run ``execute``
with the query, call one of :meth:`hazelcast.db.Cursor.fetchone`,
:meth:`hazelcast.db.Cursor.fetchmany` or :meth:`hazelcast.db.Cursor.fetchall`
to get one, some or all rows in the result. The rows are of the
:class:`hazelcast.sql.SqlRow` type. Note that, ``fetchall`` should only be used
for small, finite set of rows.

.. code:: python

    cursor.execute("SELECT * FROM stocks")
    one_row = cursor.fetchone()
    three_more_rows = cursor.fetchmany(3)
    rest_of_rows = cursor.fetchall()

Alternatively, you can iterate on the cursor itself.

.. code:: python

    cursor.execute("SELECT * FROM stocks")
    for row in cursor:
        # handle the row

You can access columns in a :class:`hazelcast.sql.SqlRow` by using the subscription
notation, treating the row as a dictionary.

.. code:: python

    for row in cursor:
        print(row["__key"], row["symbol"], row["price"])

Alternatively, you can treat the row as an array and use indexes
to access column values.

.. code:: python

    for row in cursor:
        print(row[0], row[1], row[2])


Once you are done with the cursor, you can use its
:meth:`hazelcast.db.Cursor.close` method to release its resources.

.. code:: python

    cursor.close()

Using the ``with`` statement, ``close`` is called automatically:

.. code:: python

    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM stocks")
        for row in cursor:
            # handle the row
    # cursor is automatically closed here.

