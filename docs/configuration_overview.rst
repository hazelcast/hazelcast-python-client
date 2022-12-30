Configuration Overview
======================

The client can be configured either by keyword arguments or by a configuration
object.

Keyword Arguments Configuration
-------------------------------

It is possible to pass keyword arguments directly to the client's constructor
to configure desired aspects of the client.

The keyword argument names must be valid property names of the
:class:`hazelcast.config.Config` class with valid values.

.. code:: python

    from hazelcast import HazelcastClient

    client = HazelcastClient(
        cluster_name="a-cluster",
        cluster_members=["127.0.0.1:5701"],
    )


Using a Configuration Object
----------------------------

Alternatively, you can create a configuration object, and pass it to the client
as its only argument.

This way might provide better user experience as it provides hints for the
configuration option names and their types.

.. code:: python

    from hazelcast import HazelcastClient
    from hazelcast.config import Config

    config = Config()
    config.cluster_name = "a-cluster"
    config.cluster_members = ["127.0.0.1:5701"]
    client = HazelcastClient(config)
