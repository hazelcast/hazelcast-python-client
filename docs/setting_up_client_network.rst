Setting Up Client Network
=========================

Main parts of network related configuration for Hazelcast Python client
may be tuned via the arguments described in this section.

Here is an example of configuring the network for Python client.

.. code:: python

    client = hazelcast.HazelcastClient(
        cluster_members=[
            "10.1.1.21",
            "10.1.1.22:5703"
        ],
        smart_routing=True,
        redo_operation=False,
        connection_timeout=6.0
    )

Providing Member Addresses
--------------------------

Address list is the initial list of cluster addresses which the client
will connect to. The client uses this list to find an alive member.
Although it may be enough to give only one address of a member in the
cluster (since all members communicate with each other), it is
recommended that you give the addresses for all the members.

.. code:: python

    client = hazelcast.HazelcastClient(
        cluster_members=[
            "10.1.1.21",
            "10.1.1.22:5703"
        ]
    )

If the port part is omitted, then ``5701``, ``5702`` and ``5703`` will
be tried in a random order.

You can specify multiple addresses with or without the port information
as seen above. The provided list is shuffled and tried in a random
order. Its default value is ``localhost``.

Setting Smart Routing
---------------------

Smart routing defines whether the client mode is smart or unisocket. See
the
:ref:`using_python_client_with_hazelcast_imdg:python client operation modes`
section for the description of smart and unisocket modes.

.. code:: python

    client = hazelcast.HazelcastClient(
        smart_routing=True,
    )

Its default value is ``True`` (smart client mode).

Enabling Redo Operation
-----------------------

It enables/disables redo-able operations. While sending the requests to
the related members, the operations can fail due to various reasons.
Read-only operations are retried by default. If you want to enable retry
for the other operations, you can set the ``redo_operation`` to
``True``.

.. code:: python

    client = hazelcast.HazelcastClient(
        redo_operation=False
    )

Its default value is ``False`` (disabled).

Setting Connection Timeout
--------------------------

Connection timeout is the timeout value in seconds for the members to
accept the client connection requests.

.. code:: python

    client = hazelcast.HazelcastClient(
        connection_timeout=6.0
    )

Its default value is ``5.0`` seconds.

Enabling Client TLS/SSL
-----------------------

You can use TLS/SSL to secure the connection between the clients and
members. If you want to enable TLS/SSL for the client-cluster
connection, you should set the SSL configuration. Please see the
:ref:`securing_client_connection:tls/ssl` section.

As explained in the :ref:`securing_client_connection:tls/ssl` section,
Hazelcast members have key stores used to identify themselves
(to other members) and Hazelcast Python clients have certificate
authorities used to define which members they can trust. Hazelcast has
the mutual authentication feature which allows the Python clients also
to have their private keys and public certificates, and members to have
their certificate authorities so that the members can know which
clients they can trust. See the
:ref:`securing_client_connection:mutual authentication` section.

Enabling Hazelcast Cloud Discovery
----------------------------------

Hazelcast Python client can discover and connect to Hazelcast clusters
running on `Hazelcast Cloud <https://cloud.hazelcast.com/>`__. For this,
provide authentication information as ``cluster_name`` and enable cloud
discovery by setting your ``cloud_discovery_token`` as shown below.

.. code:: python

    client = hazelcast.HazelcastClient(
        cluster_name="name-of-your-cluster",
        cloud_discovery_token="discovery-token"
    )

If you have enabled encryption for your cluster, you should also enable
TLS/SSL configuration for the client to secure communication between
your client and cluster members as described in the
:ref:`securing_client_connection:tls/ssl for hazelcast python clients`
section.

Configuring Backup Acknowledgment
---------------------------------

When an operation with sync backup is sent by a client to the Hazelcast
member(s), the acknowledgment of the operation’s backup is sent to the
client by the backup replica member(s). This improves the performance of
the client operations.

To disable backup acknowledgement, you should use the
``backup_ack_to_client_enabled`` configuration option.

.. code:: python

    client = hazelcast.HazelcastClient(
        backup_ack_to_client_enabled=False,
    )

Its default value is ``True``. This option has no effect for unisocket
clients.

You can also fine-tune this feature using the config options as
described below:

- ``operation_backup_timeout``: Default value is ``5`` seconds. If an
  operation has backups, this property specifies how long the
  invocation waits for acks from the backup replicas. If acks are not
  received from some of the backups, there will not be any rollback on
  the other successful replicas.

- ``fail_on_indeterminate_operation_state``: Default value is
  ``False``. When it is ``True``, if an operation has sync backups and
  acks are not received from backup replicas in time, or the member
  which owns primary replica of the target partition leaves the
  cluster, then the invocation fails. However, even if the invocation
  fails, there will not be any rollback on other successful replicas.
