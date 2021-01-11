Hazelcast Python Client
=======================

.. image:: https://img.shields.io/pypi/v/hazelcast-python-client
    :target: https://pypi.org/project/hazelcast-python-client/
    :alt: PyPI
.. image:: https://img.shields.io/static/v1?label=Github&message=Hazelcast%20Python%20client&style=flat&logo=github
    :target: https://github.com/hazelcast/hazelcast-python-client
    :alt: Github Repository
.. image:: https://hz-community-slack.herokuapp.com/badge.svg
    :target: https://slack.hazelcast.com
    :alt: Join the community on Slack
.. image:: https://img.shields.io/pypi/l/hazelcast-python-client
    :target: https://github.com/hazelcast/hazelcast-python-client/blob/master/LICENSE.txt
    :alt: License

----

`Hazelcast <https://hazelcast.org/>`__ is an open-source distributed
in-memory data store and computation platform that provides a wide
variety of distributed data structures and concurrency primitives.

Hazelcast Python client is a way to communicate to Hazelcast IMDG
clusters and access the cluster data. The client provides a
Future-based asynchronous API suitable for wide ranges of use cases.


Overview
--------

Usage
~~~~~

.. code:: python

    import hazelcast

    # Connect to Hazelcast cluster.
    client = hazelcast.HazelcastClient()

    # Get or create the "distributed-map" on the cluster.
    distributed_map = client.get_map("distributed-map")

    # Put "key", "value" pair into the "distributed-map" and wait for
    # the request to complete.
    distributed_map.set("key", "value").result()

    # Try to get the value associated with the given key from the cluster
    # and attach a callback to be executed once the response for the
    # get request is received. Note that, the set request above was
    # blocking since it calls ".result()" on the returned Future, whereas
    # the get request below is non-blocking.
    get_future = distributed_map.get("key")
    get_future.add_done_callback(lambda future: print(future.result()))

    # Do other operations. The operations below won't wait for
    # the get request above to complete.

    print("Map size:", distributed_map.size().result())

    # Shutdown the client.
    client.shutdown()


If you are using Hazelcast IMDG and the Python client on the same
machine, the default configuration should work out-of-the-box. However,
you may need to configure the client to connect to cluster nodes that
are running on different machines or to customize client properties.

Configuration
~~~~~~~~~~~~~

.. code:: python

    import hazelcast

    client = hazelcast.HazelcastClient(
        cluster_name="cluster-name",
        cluster_members=[
            "10.90.0.2:5701",
            "10.90.0.3:5701",
        ],
        lifecycle_listeners=[
            lambda state: print("Lifecycle event >>>", state),
        ]
    )

    print("Connected to cluster")
    client.shutdown()


See the API documentation of :class:`hazelcast.client.HazelcastClient`
to learn more about supported configuration options.

Features
--------

-  Distributed, partitioned and queryable in-memory key-value store
   implementation, called **Map**
-  Eventually consistent cache implementation to store a subset of the
   Map data locally in the memory of the client, called **Near Cache**
-  Additional data structures and simple messaging constructs such as
   **Set**, **MultiMap**, **Queue**, **Topic**
-  Cluster-wide unique ID generator, called **FlakeIdGenerator**
-  Distributed, CRDT based counter, called **PNCounter**
-  Distributed concurrency primitives from CP Subsystem such as
   **FencedLock**, **Semaphore**, **AtomicLong**
-  Integration with `Hazelcast Cloud <https://cloud.hazelcast.com/>`__
-  Support for serverless and traditional web service architectures with
   **Unisocket** and **Smart** operation modes
-  Ability to listen client lifecycle, cluster state and distributed
   data structure events
-  and `many
   more <https://hazelcast.org/imdg/clients-languages/python/#client-features>`__


.. toctree::
    :hidden:

    client
    api/modules
    getting_started
    features
    configuration_overview
    serialization
    setting_up_client_network
    client_connection_strategy
    using_python_client_with_hazelcast_imdg
    securing_client_connection
    development_and_testing
    getting_help
    contributing
    license
    copyright