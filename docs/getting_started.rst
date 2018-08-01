Getting Started
===============

Hazelcast Python Client is a Hazelcast Client Protocol implementation for Python 2.7, 3.4, 3.5, 3.6 and 3.7.


Installation
------------

You can install the Hazelcast Python client simply by executing either of the following commands::

    python setup.py install

An even simpler way is to install the client from the official Python Package Index::

    pip install hazelcast-python-client



Starting Hazelcast
------------------

Before diving right into the configuration, you should have a running Hazelcast server. If you are already familiar
with Hazelcast and have a server running, you can simply skip to the Configuration section.

If not, follow these steps to start an Hazelcast server in your local computer:

* Make sure that you have `Java <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`_ installed on your system.
* Download the latest `Hazelcast release <https://hazelcast.org/download/>`_.
* Extract the zip file you have downloaded.
* You should see a directory called **bin** which includes a few scripts and xml files.
* If you are using Linux/MacOS, you can start a Hazelcast member with **start.sh** and stop the member you have started with **stop.sh** when you are done.
* If you are using Windows, you can start a Hazelcast member with **start.bat** and stop the member you have started with **stop.bat** when you are done.
* Refer to the `Using the Scripts In The Package <http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#using-the-scripts-in-the-package>`_ for more information about these scripts.
* After successfully running these scripts, you should see the ip address and port of your member on your console. Take a note of these and jump right into the Configuration section!


Configuration
-------------

As the first step, you need a simple configuration. This configuration provides information on how to
connect to an already existing Hazelcast cluster and looks as simple as the following example:

.. code-block:: python

    import hazelcast, logging

    config = hazelcast.ClientConfig()
    # Hazelcast.Address is the hostname or IP address, e.g. 'localhost:5701'
    config.network_config.addresses.append('Hazelcast.Address')

    # basic logging setup to see client logs
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)


Starting the client and accessing the Map
-----------------------------------------

After the configuration, you can initialize the client as follows:

.. code-block:: python

    client = hazelcast.HazelcastClient(config)

The client object should be created once unless you connect to multiple clusters.
And, it should be eventually shut down after using the method ``client.shutdown()``.

With our newly created access to the Hazelcast cluster, we now want to read and write data. Same as before, Hazelcast is as
simple as possible and access to the distributed map is granted again by a single command:

.. code-block:: python

    my_async_map = client.get_map("map-name")

The Python client is designed to be fully asynchronous. Every distributed object function such as map.put, map.get, etc. will
return a future to you which works the same way as the Python 3 ``future`` class.

You can simply retrieve the operations result from the future:

.. code-block:: python

    future = my_map.put("key", "async_val")
    old_value = future.result()

Or use it to register callback methods which will be executed asynchronously:

.. code-block:: python

    def get_async_callback(f):
        print("map.get_async:", f.result())

    future = my_map.get("key")
    future.add_done_callback(get_async_callback)

Although async operations are more efficient in a single threaded Python interpreter, we sometimes need a simpler code.
Python client provides a convenience method to support blocking methods.

Every distributed object provides a blocking helper function as shown below:

.. code-block:: python

    my_map = client.get_map("map-name").blocking()

Our map implementation is still completely asynchronous internally, but the blocking helper function will call the method result()
and return the result instead.

.. code-block:: python

    my_map.put("key_1", "value_1")
    value = my_map.get("key_1")

Please note that this time the result is returned instead of the future object, compared to the previous example.


