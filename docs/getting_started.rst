Getting Started
===============

This chapter provides information on how to get started with your
Hazelcast Python client. It outlines the requirements, installation and
configuration of the client, setting up a cluster, and provides a simple
application that uses a distributed map in Python client.

Requirements
------------

- Windows, Linux/UNIX or Mac OS X
- Python 3.6 or newer
- Java 8 or newer
- Hazelcast 4.0 or newer
- Latest Hazelcast Python client

Working with Hazelcast Clusters
-------------------------------

Hazelcast Python client requires a working Hazelcast cluster to
run. This cluster handles storage and manipulation of the user data.
Clients are a way to connect to the Hazelcast cluster and access
such data.

Hazelcast cluster consists of one or more cluster members. These
members generally run on multiple virtual or physical machines and are
connected to each other via network. Any data put on the cluster is
partitioned to multiple members transparent to the user. It is therefore
very easy to scale the system by adding new members as the data grows.
Hazelcast cluster also offers resilience. Should any hardware or
software problem causes a crash to any member, the data on that member
is recovered from backups and the cluster continues to operate without
any downtime. Hazelcast clients are an easy way to connect to a
Hazelcast cluster and perform tasks on distributed data structures
that live on the cluster.

In order to use Hazelcast Python client, we first need to setup a
Hazelcast cluster.

Setting Up a Hazelcast Cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are following options to start a Hazelcast cluster easily:

- You can use our `Docker
  images <https://hub.docker.com/r/hazelcast/hazelcast/>`__.

  .. code:: bash

      docker run -p 5701:5701 hazelcast/hazelcast:5.0

- You can use `Hazelcast CLI
  <https://docs.hazelcast.com/hazelcast/latest/getting-started/install-hazelcast#using-a-package-manager>`__.
- You can run standalone members by downloading and running distribution
  files from the website.
- You can embed members to your Java projects.

We are going to download distribution files from the website and run
a standalone member for this guide.

Running Standalone JARs
^^^^^^^^^^^^^^^^^^^^^^^

Follow the instructions below to create a Hazelcast cluster:

1. Go to Hazelcast’s download `page
   <https://hazelcast.com/open-source-projects/downloads/>`__
   and download either the ``.zip`` or ``.tar`` distribution of Hazelcast.
2. Decompress the contents into any directory that you want to run
   members from.
3. Change into the directory that you decompressed the Hazelcast content
   and then into the ``bin`` directory.
4. Use either ``hz-start`` or ``hz-start.bat`` depending on your operating
   system. Once you run the start script, you should see the Hazelcast
   logs in the terminal.

You should see a log similar to the following, which means that your
1-member cluster is ready to be used:

::

    Sep 03, 2020 2:21:57 PM com.hazelcast.core.LifecycleService
    INFO: [192.168.1.10]:5701 [dev] [4.1-SNAPSHOT] [192.168.1.10]:5701 is STARTING
    Sep 03, 2020 2:21:58 PM com.hazelcast.internal.cluster.ClusterService
    INFO: [192.168.1.10]:5701 [dev] [4.1-SNAPSHOT]

    Members {size:1, ver:1} [
        Member [192.168.1.10]:5701 - 7362c66f-ef9f-4a6a-a003-f8b33dfd292a this
    ]

    Sep 03, 2020 2:21:58 PM com.hazelcast.core.LifecycleService
    INFO: [192.168.1.10]:5701 [dev] [4.1-SNAPSHOT] [192.168.1.10]:5701 is STARTED

Adding User Library to CLASSPATH
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When you want to use features such as querying and language
interoperability, you might need to add your own Java classes to the
Hazelcast member in order to use them from your Python client. This can
be done by adding your own compiled code to the ``CLASSPATH``. To do
this, compile your code with the ``CLASSPATH`` and add the compiled
files to the ``user-lib`` directory in the extracted
``hazelcast-<version>.zip`` (or ``tar``). Then, you can start your
Hazelcast member by using the start scripts in the ``bin`` directory.
The start scripts will automatically add your compiled classes to the
``CLASSPATH``.

Note that if you are adding an ``IdentifiedDataSerializable`` or a
``Portable`` class, you need to add its factory too. Then you should
configure the factory in the ``hazelcast.xml`` configuration file. This
file resides in the ``bin`` directory where you extracted the
``hazelcast-<version>.zip`` (or ``tar``).

The following is an example configuration when you are adding an
``IdentifiedDataSerializable`` class:

.. code:: xml

    <hazelcast>
        ...
        <serialization>
           <data-serializable-factories>
               <data-serializable-factory factory-id=<identified-factory-id>>
                   IdentifiedFactoryClassName
               </data-serializable-factory>
           </data-serializable-factories>
        </serialization>
        ...
    </hazelcast>

If you want to add a ``Portable`` class, you should use
``<portable-factories>`` instead of ``<data-serializable-factories>`` in
the above configuration.

See the `Hazelcast Reference Manual
<https://docs.hazelcast.com/hazelcast/latest/getting-started/install-hazelcast>`__
for more information on setting up the clusters.

Downloading and Installing
--------------------------

You can download and install the Python client from
`PyPI <https://pypi.org/project/hazelcast-python-client/>`__ using pip.
Run the following command:

::

    pip install hazelcast-python-client

Alternatively, it can be installed from the source using the following
command:

::

    python setup.py install

Basic Configuration
-------------------

If you are using Hazelcast and Python client on the same computer,
generally the default configuration should be fine. This is great for
trying out the client. However, if you run the client on a different
computer than any of the cluster members, you may need to do some simple
configurations such as specifying the member addresses.

The Hazelcast members and clients have their own configuration
options. You may need to reflect some of the member side configurations
on the client side to properly connect to the cluster.

This section describes the most common configuration elements to get you
started in no time. It discusses some member side configuration options
to ease the understanding of Hazelcast’s ecosystem. Then, the client
side configuration options regarding the cluster connection are
discussed. The configurations for the Hazelcast data structures
that can be used in the Python client are discussed in the following
sections.

See the `Hazelcast Reference Manual
<https://docs.hazelcast.com/hazelcast/latest/>`__
and :ref:`configuration_overview:configuration overview` section for
more information.

Configuring Hazelcast
~~~~~~~~~~~~~~~~~~~~~

Hazelcast aims to run out-of-the-box for most common scenarios.
However if you have limitations on your network such as multicast being
disabled, you may have to configure your Hazelcast members so that
they can find each other on the network. Also, since most of the
distributed data structures are configurable, you may want to configure
them according to your needs. We will show you the basics about network
configuration here.

You can use the following options to configure Hazelcast:

- Using the ``hazelcast.xml`` configuration file.
- Programmatically configuring the member before starting it from the
  Java code.

Since we use standalone servers, we will use the ``hazelcast.xml`` file
to configure our cluster members.

When you download and unzip ``hazelcast-<version>.zip`` (or ``tar``),
you see the ``hazelcast.xml`` in the ``bin`` directory. When a Hazelcast
member starts, it looks for the ``hazelcast.xml`` file to load the
configuration from. A sample ``hazelcast.xml`` is shown below.

.. code:: xml

    <hazelcast>
        <cluster-name>dev</cluster-name>
        <network>
            <port auto-increment="true" port-count="100">5701</port>
            <join>
                <multicast enabled="true">
                    <multicast-group>224.2.2.3</multicast-group>
                    <multicast-port>54327</multicast-port>
                </multicast>
                <tcp-ip enabled="false">
                    <interface>127.0.0.1</interface>
                    <member-list>
                        <member>127.0.0.1</member>
                    </member-list>
                </tcp-ip>
            </join>
            <ssl enabled="false"/>
        </network>
        <partition-group enabled="false"/>
        <map name="default">
            <backup-count>1</backup-count>
        </map>
    </hazelcast>

We will go over some important configuration elements in the rest of
this section.

- ``<cluster-name>``: Specifies which cluster this member belongs to. A
  member connects only to the other members that are in the same
  cluster as itself. You may give your clusters different names so that
  they can live in the same network without disturbing each other. Note
  that the cluster name should be the same across all members and
  clients that belong to the same cluster.
- ``<network>``

  - ``<port>``: Specifies the port number to be used by the member
    when it starts. Its default value is 5701. You can specify another
    port number, and if you set ``auto-increment`` to ``true``, then
    Hazelcast will try the subsequent ports until it finds an
    available port or the ``port-count`` is reached.
  - ``<join>``: Specifies the strategies to be used by the member to
    find other cluster members. Choose which strategy you want to use
    by setting its ``enabled`` attribute to ``true`` and the others to
    ``false``.

    - ``<multicast>``: Members find each other by sending multicast
      requests to the specified address and port. It is very useful
      if IP addresses of the members are not static.
    - ``<tcp>``: This strategy uses a pre-configured list of known
      members to find an already existing cluster. It is enough for a
      member to find only one cluster member to connect to the
      cluster. The rest of the member list is automatically retrieved
      from that member. We recommend putting multiple known member
      addresses there to avoid disconnectivity should one of the
      members in the list is unavailable at the time of connection.

These configuration elements are enough for most connection scenarios.
Now we will move onto the configuration of the Python client.

Configuring Hazelcast Python Client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To configure your Hazelcast Python client, you need to pass
configuration options as keyword arguments to your client at the
startup. The names of the configuration options is similar to
``hazelcast.xml`` configuration file used when configuring the member,
but flatter. It is done this way to make it easier to transfer Hazelcast
skills to multiple platforms.

This section describes some network configuration settings to cover
common use cases in connecting the client to a cluster. See the
:ref:`configuration_overview:configuration overview` section and the
following sections for information about detailed network
configurations and/or additional features of Hazelcast Python client
configuration.

.. code:: python

    import hazelcast

    client = hazelcast.HazelcastClient(
        cluster_members=[
            "some-ip-address:port"
        ],
        cluster_name="name-of-your-cluster",
    )

It’s also possible to omit the keyword arguments in order to use the
default settings.

.. code:: python

    import hazelcast

    client = hazelcast.HazelcastClient()

If you run the Hazelcast members on a different server than the
client, you most probably have configured the members’ ports and cluster
names as explained in the previous section. If you did, then you need to
make match those changes to the network settings of your client.

Cluster Name Setting
^^^^^^^^^^^^^^^^^^^^

You need to provide the name of the cluster, if it is defined on the
server side, to which you want the client to connect.

.. code:: python

    import hazelcast

    client = hazelcast.HazelcastClient(
        cluster_name="name-of-your-cluster",
    )

Network Settings
^^^^^^^^^^^^^^^^

You need to provide the IP address and port of at least one member in
your cluster so the client can find it.

.. code:: python

    import hazelcast

    client = hazelcast.HazelcastClient(
        cluster_members=["some-ip-address:port"]
    )

Basic Usage
-----------

Now that we have a working cluster and we know how to configure both our
cluster and client, we can run a simple program to use a distributed map
in the Python client.

.. code:: python

    import logging
    import hazelcast

    # Enable logging to see the logs
    logging.basicConfig(level=logging.INFO)

    # Connect to Hazelcast cluster
    client = hazelcast.HazelcastClient()

    client.shutdown()

This should print logs about the cluster members such as address, port
and UUID to the ``stderr``.

::

    INFO:hazelcast.lifecycle:HazelcastClient 4.0.0 is STARTING
    INFO:hazelcast.lifecycle:HazelcastClient 4.0.0 is STARTED
    INFO:hazelcast.connection:Trying to connect to Address(host=127.0.0.1, port=5701)
    INFO:hazelcast.lifecycle:HazelcastClient 4.0.0 is CONNECTED
    INFO:hazelcast.connection:Authenticated with server Address(host=172.17.0.2, port=5701):7682c357-3bec-4841-b330-6f9ae0c08253, server version: 4.0, local address: Address(host=127.0.0.1, port=56718)
    INFO:hazelcast.cluster:

    Members [1] {
        Member [172.17.0.2]:5701 - 7682c357-3bec-4841-b330-6f9ae0c08253
    }

    INFO:hazelcast.client:Client started
    INFO:hazelcast.lifecycle:HazelcastClient 4.0.0 is SHUTTING_DOWN
    INFO:hazelcast.connection:Removed connection to Address(host=127.0.0.1, port=5701):7682c357-3bec-4841-b330-6f9ae0c08253, connection: Connection(id=0, live=False, remote_address=Address(host=172.17.0.2, port=5701))
    INFO:hazelcast.lifecycle:HazelcastClient 4.0.0 is DISCONNECTED
    INFO:hazelcast.lifecycle:HazelcastClient 4.0.0 is SHUTDOWN

Congratulations. You just started a Hazelcast Python client.

**Using a Map**

Let’s manipulate a distributed map (similar to Python’s builtin ``dict``)
on a cluster using the client.

.. code:: python

    import hazelcast

    client = hazelcast.HazelcastClient()

    personnel_map = client.get_map("personnel-map")
    personnel_map.put("Alice", "IT")
    personnel_map.put("Bob", "IT")
    personnel_map.put("Clark", "IT")

    print("Added IT personnel. Printing all known personnel")

    for person, department in personnel_map.entry_set().result():
        print("%s is in %s department" % (person, department))

    client.shutdown()

**Output**

::

    Added IT personnel. Printing all known personnel
    Alice is in IT department
    Clark is in IT department
    Bob is in IT department

You see this example puts all the IT personnel into a cluster-wide
``personnel-map`` and then prints all the known personnel.

Now, run the following code.

.. code:: python

    import hazelcast

    client = hazelcast.HazelcastClient()

    personnel_map = client.get_map("personnel-map")
    personnel_map.put("Denise", "Sales")
    personnel_map.put("Erwing", "Sales")
    personnel_map.put("Faith", "Sales")

    print("Added Sales personnel. Printing all known personnel")

    for person, department in personnel_map.entry_set().result():
        print("%s is in %s department" % (person, department))

    client.shutdown()

**Output**

::

    Added Sales personnel. Printing all known personnel
    Denise is in Sales department
    Erwing is in Sales department
    Faith is in Sales department
    Alice is in IT department
    Clark is in IT department
    Bob is in IT department


.. Note:: For the sake of brevity we are going to omit boilerplate
    parts, like ``import``\s, in the later code snippets. Refer to
    the :ref:`getting_started:code samples` section to see samples
    with the complete code.


You will see this time we added only the sales employees but we got the
list of all known employees including the ones in IT. This is because
our map lives in the cluster and no matter which client we use, we can
access the whole map.

You may wonder why we have used ``result()`` method over the
``entry_set()`` method of the ``personnel_map``. This is because the
Hazelcast Python client is designed to be fully asynchronous. Every
method call over distributed objects such as ``put()``, ``get()``,
``entry_set()``, etc. will return a ``Future`` object that is similar to
the
`Future <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future>`__
class of the
`concurrent.futures <https://docs.python.org/3/library/concurrent.futures.html#module-concurrent.futures>`__
module.

With this design choice, method calls over the distributed objects can
be executed asynchronously without blocking the execution order of your
program.

You may get the value returned by the method calls using the
``result()`` method of the ``Future`` class. This will block the
execution of your program and will wait until the future finishes
running. Then, it will return the value returned by the call which are
key-value pairs in our ``entry_set()`` method call.

You may also attach a function to the future objects that will be
called, with the future as its only argument, when the future finishes
running.

For example, the part where we printed the personnel in above code can
be rewritten with a callback attached to the ``entry_set()``, as shown
below..

.. code:: python

    def entry_set_cb(future):
        for person, department in future.result():
            print("%s is in %s department" % (person, department))


    personnel_map.entry_set().add_done_callback(entry_set_cb)
    time.sleep(1)  # wait for Future to complete

Asynchronous operations are far more efficient in single threaded Python
interpreter but you may want all of your method calls over distributed
objects to be blocking. For this purpose, Hazelcast Python client
provides a helper method called ``blocking()``. This method blocks the
execution of your program for all the method calls over distributed
objects until the return value of your call is calculated and returns
that value directly instead of a ``Future`` object.

To make the ``personnel_map`` presented previously in this section
blocking, you need to call ``blocking()`` method over it.

.. code:: python

    personnel_map = client.get_map("personnel-map").blocking()

Now, all the methods over the ``personnel_map``, such as ``put()`` and
``entry_set()``, will be blocking. So, you don’t need to call
``result()`` over it or attach a callback to it anymore.

.. code:: python

    for person, department in personnel_map.entry_set():
        print("%s is in %s department" % (person, department))

Code Samples
------------

See the Hazelcast Python
`examples <https://github.com/hazelcast/hazelcast-python-client/tree/master/examples>`__
for more code samples.
