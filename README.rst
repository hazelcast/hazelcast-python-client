Hazelcast Python Client
=======================

.. image:: https://img.shields.io/pypi/v/hazelcast-python-client
    :target: https://pypi.org/project/hazelcast-python-client/
    :alt: PyPI
.. image:: https://img.shields.io/readthedocs/hazelcast
    :target: https://hazelcast.readthedocs.io
    :alt: Read the Docs
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

Installation
------------

Hazelcast
~~~~~~~~~

Hazelcast Python client requires a working Hazelcast IMDG cluster to
run. This cluster handles the storage and manipulation of the user data.

A Hazelcast IMDG cluster consists of one or more cluster members. These
members generally run on multiple virtual or physical machines and are
connected to each other via the network. Any data put on the cluster is
partitioned to multiple members transparent to the user. It is therefore
very easy to scale the system by adding new members as the data grows.
Hazelcast IMDG cluster also offers resilience. Should any hardware or
software problem causes a crash to any member, the data on that member
is recovered from backups and the cluster continues to operate without
any downtime.

The quickest way to start a single member cluster for development
purposes is to use our `Docker
images <https://hub.docker.com/r/hazelcast/hazelcast/>`__.

.. code:: bash

   docker run -p 5701:5701 hazelcast/hazelcast:4.1

You can also use our ZIP or TAR
`distributions <https://hazelcast.org/imdg/download/archives/#hazelcast-imdg>`__.
Once you have downloaded, you can start the Hazelcast member using
the ``bin/start.sh`` script.

Client
~~~~~~

.. code:: bash

   pip install hazelcast-python-client

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


Refer to `the documentation <https://hazelcast.readthedocs.io>`__
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
-  Ability to listen to client lifecycle, cluster state, and distributed
   data structure events
-  and `many
   more <https://hazelcast.org/imdg/clients-languages/python/#client-features>`__

Getting Help
------------

You can use the following channels for your questions and
development/usage issues:

-  `GitHub
   repository <https://github.com/hazelcast/hazelcast-python-client/issues/new>`__
-  `Documentation <https://hazelcast.readthedocs.io>`__
-  `Slack <https://slack.hazelcast.com>`__
-  `Google Groups <https://groups.google.com/g/hazelcast>`__
-  `Stack
   Overflow <https://stackoverflow.com/questions/tagged/hazelcast>`__

Contributing
------------

We encourage any type of contribution in the form of issue reports or
pull requests.

Issue Reports
~~~~~~~~~~~~~

For issue reports, please share the following information with us to
quickly resolve the problems:

-  Hazelcast IMDG and the client version that you use
-  General information about the environment and the architecture you
   use like Python version, cluster size, number of clients, Java
   version, JVM parameters, operating system etc.
-  Logs and stack traces, if any
-  Detailed description of the steps to reproduce the issue

Pull Requests
~~~~~~~~~~~~~

Contributions are submitted, reviewed and accepted using the pull
requests on GitHub. For an enhancement or larger feature, please
create a GitHub issue first to discuss.

Development
^^^^^^^^^^^

1. Clone the `GitHub repository
   <https://github.com/hazelcast/hazelcast-python-client.git>`__.
2. Run ``python setup.py install`` to install the Python client.

If you are planning to contribute:

1. Run ``pip install -r requirements-dev.txt`` to install development
   dependencies.
2. Use `black <https://pypi.org/project/black/>`__ to reformat the code
   by running the ``black --config black.toml .`` command.
3. Make sure that tests are passing by following the steps described
   in the next section.

Testing
^^^^^^^

In order to test Hazelcast Node.js client locally, you will need the
following:

-  Java 8 or newer
-  Maven

Following commands starts the tests according to your operating system:

.. code:: bash

    bash run-tests.sh

or

.. code:: powershell

    .\run-tests.ps1

Test script automatically downloads ``hazelcast-remote-controller`` and
Hazelcast IMDG. The script uses Maven to download those.

License
-------

`Apache 2.0 License <LICENSE>`__.

Copyright
---------

Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.

Visit `www.hazelcast.com <http://www.hazelcast.com>`__ for more
information.
