Getting Started - Asyncio API (BETA)
====================================

.. warning::

    Hazelcast's asyncio API is BETA. Do not use it in production.

This chapter provides information on how to get started with the Hazelcast Python client using its asyncio API.
It provides the requirements, installation instructions, and a simple application that uses a distributed map in a Python client.

Requirements
------------

- Windows, Linux, macOS
- Python 3.11 or newer
- Hazelcast 5.6 or newer
- `Supported Java Development Kit (JDK) <https://docs.hazelcast.com/hazelcast/latest/deploy/versioning-compatibility#java-development-kits-jdks>`__
- Latest Hazelcast Python client

Working with Hazelcast Clusters
-------------------------------

The Hazelcast Python client requires a working Hazelcast cluster to run.
The cluster handles storage and manipulation of user data.

There are several options to run a Hazelcast cluster:

- `Docker <https://docs.hazelcast.com/hazelcast/latest/getting-started/get-started-docker>`__
- `Hazelcast CLI (hz) <https://docs.hazelcast.com/hazelcast/latest/getting-started/get-started-cli>`__
- `Binary <https://docs.hazelcast.com/hazelcast/latest/getting-started/get-started-binary>`__
- `Hazelcast Enterprise <https://docs.hazelcast.com/hazelcast/latest/getting-started/get-started-enterprise>`__

Once the Hazelcast cluster is running, you can carry on with creating and starting the Hazelcast client.

Working with the Hazelcast Client
---------------------------------

Creating and Starting Client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The asyncio client-specific public API is exposed in the ``hazelcast.asyncio`` package.
Call the ``create_and_start`` method of the ``HazelcastClient`` class to create an instance and connect it to the cluster.

.. code:: python

    import asyncio

    from hazelcast.asyncio import HazelcastClient

    async def amain():
        client = await HazelcastClient.create_and_start()
        # ... use the client ...

    asyncio.run(amain())

The Hazelcast client requires an event loop to be running.
Additionally, any code that uses the ``await`` keyword must be put in an ``async`` function.

Configuring the Client
~~~~~~~~~~~~~~~~~~~~~~

You must pass configuration options as keyword arguments to your client at startup.

This section describes some network options to cover common use cases for connecting the client to a cluster.
See the :ref:`configuration_overview:configuration overview` section for information.

You can omit the keyword arguments to use the default settings:

.. code:: python

    client = await HazelcastClient.create_and_start()

Cluster Name Setting
^^^^^^^^^^^^^^^^^^^^

If the cluster was configured with a name other than the default ``dev``, the same name must be used in the client configuration:

.. code:: python

    client = await hazelcast.HazelcastClient(
        cluster_name="name-of-your-cluster",
    )

Network Settings
^^^^^^^^^^^^^^^^

You need to provide the IP address and port of at least one member in
your cluster so the client can find it.

.. code:: python

    client = await hazelcast.HazelcastClient.create_and_start(
        cluster_members=["some-ip-address:port"]
    )

Basic Usage
-----------

Run a simple program to use a distributed map in the Python client:

.. code:: python

    import asyncio
    import logging

    from hazelcast.asyncio import HazelcastClient

    # Enable logging to see the logs
    logging.basicConfig(level=logging.INFO)

    async def amain():
        # Connect to Hazelcast cluster
        client = await HazelcastClient.create_and_start()
        await client.shutdown()

    asyncio.run(amain())

This should print logs about the cluster members such as address, port
and UUID to the ``stderr``.

.. code:: text

    INFO:hazelcast.lifecycle:HazelcastClient 5.6.0 is STARTING
    INFO:hazelcast.lifecycle:HazelcastClient 5.6.0 is STARTED
    INFO:hazelcast.internal.asyncio_connection:Trying to connect to Address(host=127.0.0.1, port=5701)
    INFO:hazelcast.lifecycle:HazelcastClient 5.6.0 is CONNECTED
    INFO:hazelcast.internal.asyncio_connection:Authenticated with server Address(host=127.0.0.1, port=5701):49c5407d-78f4-4611-a025-95f7afd0ab68, server version: 5.5.2, local address: Address(host=127.0.0.1, port=56134)
    INFO:hazelcast.internal.asyncio_cluster:

    Members [1] {
        Member [127.0.0.1]:5701 - 49c5407d-78f4-4611-a025-95f7afd0ab68
    }

    INFO:hazelcast.internal.asyncio_client:Client started
    INFO:hazelcast.lifecycle:HazelcastClient 5.6.0 is SHUTTING_DOWN
    INFO:hazelcast.internal.asyncio_connection:Removed connection to Address(host=127.0.0.1, port=5701):49c5407d-78f4-4611-a025-95f7afd0ab68, connection: <hazelcast.internal.asyncio_reactor.AsyncioConnection object at 0x7605c4aa8d70>
    INFO:hazelcast.lifecycle:HazelcastClient 5.6.0 is DISCONNECTED
    INFO:hazelcast.lifecycle:HazelcastClient 5.6.0 is SHUTDOWN

Congratulations!
You just started a Hazelcast Python client instance.

**Using a Map**

Let's manipulate a distributed map on a cluster using the client.

.. code:: python

    import asyncio

    from hazelcast.asyncio import HazelcastClient

    async def amain():
        client = await HazelcastClient.create_and_start()

        personnel_map = await client.get_map("personnel-map")
        await personnel_map.put("Alice", "IT")
        await personnel_map.put("Bob", "IT")
        await personnel_map.put("Clark", "IT")

        print("Added IT personnel. Printing all known personnel")

        entries = await personnel_map.entry_set()
        for person, department in entries:
            print("%s is in %s department" % (person, department))

        await client.shutdown()

    asyncio.run(amain())

**Output**

.. code:: text

    Added IT personnel. Printing all known personnel
    Alice is in IT department
    Clark is in IT department
    Bob is in IT department

You see this example puts all the IT personnel into a cluster-wide
``personnel-map`` and then prints all the known personnel.

Now, run the following code.

.. code:: python

    import asyncio

    from hazelcast.asyncio import HazelcastClient

    async def amain():
        client = await HazelcastClient.create_and_start()
        personnel_map = await client.get_map("personnel-map")
        await personnel_map.put("Denise", "Sales")
        await personnel_map.put("Erwing", "Sales")
        await personnel_map.put("Faith", "Sales")

        print("Added Sales personnel. Printing all known personnel")

        entries = await personnel_map.entry_set()
        for person, department in entries:
            print("%s is in %s department" % (person, department))

        await client.shutdown()

    asyncio.run(amain())

**Output**

.. code:: text

    Added Sales personnel. Printing all known personnel
    Denise is in Sales department
    Erwing is in Sales department
    Faith is in Sales department
    Alice is in IT department
    Clark is in IT department
    Bob is in IT department

This time you added only the sales employees but you got the list of all known employees including the ones in IT.
That is because the map lives in the cluster and no matter which client you use, you can access the whole map.

Note that the ``await`` keyword causes the ``put`` methods to run serially.
To run them concurrently, you can wrap them in tasks.
Running the tasks in a ``TaskGroup`` makes sure the errors are handled, and all tasks are done:

.. code:: python

    import asyncio

    from hazelcast.asyncio import HazelcastClient

    async def amain():
        client = await HazelcastClient.create_and_start()
        personnel_map = await client.get_map("personnel-map")

        # update the map concurrently
        async with asyncio.TaskGroup() as tg:
            tg.create_task(personnel_map.put("Denise", "Sales"))
            tg.create_task(personnel_map.put("Erwing", "Sales"))
            tg.create_task(personnel_map.put("Faith", "Sales"))

        # all items are added to the map at this stage

        print("Added Sales personnel. Printing all known personnel")

        entries = await personnel_map.entry_set()
        for person, department in entries:
            print("%s is in %s department" % (person, department))

        await client.shutdown()

    asyncio.run(amain())

Using tasks, method calls over the distributed objects are executed asynchronously without blocking the execution order of your program.

Code Samples
------------

See the Hazelcast Python
`examples <https://github.com/hazelcast/hazelcast-python-client/tree/master/examples/asyncio>`__
for more code samples.
