Configuration Overview
======================

For configuration of the Hazelcast Python client, just pass the keyword
arguments to the client to configure the desired aspects. An example is
shown below.

.. code:: python

    client = hazelcast.HazelcastClient(
        cluster_members=["127.0.0.1:5701"]
    )

See the API documentation of :class:`hazelcast.client.HazelcastClient`
for details.