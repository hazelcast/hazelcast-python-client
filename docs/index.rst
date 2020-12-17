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


    def callback(future):
        # Outputs "value"
        print(future.result())


    distributed_map.get("key").add_done_callback(callback)

    # Do other operations. The operations below won't wait for
    # the get request above to complete.

    print("Map size:", distributed_map.size().result())

    # Shutdown the client.
    client.shutdown()



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