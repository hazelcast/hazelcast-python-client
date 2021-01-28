Client Connection Strategy
==========================

Hazelcast Python client can be configured to connect to a cluster in an
async manner during the client start and reconnecting after a cluster
disconnect. Both of these options are configured via arguments below.

You can configure the client’s starting mode as async or sync using the
configuration element ``async_start``. When it is set to ``True``
(async), the behavior of ``hazelcast.HazelcastClient()`` call changes.
It returns a client instance without waiting to establish a cluster
connection. In this case, the client rejects any network dependent
operation with ``ClientOfflineError`` immediately until it connects to
the cluster. If it is ``False``, the call is not returned and the client
is not created until a connection with the cluster is established. Its
default value is ``False`` (sync).

You can also configure how the client reconnects to the cluster after a
disconnection. This is configured using the configuration element
``reconnect_mode``; it has three options:

- ``OFF``: Client rejects to reconnect to the cluster and triggers the
  shutdown process.
- ``ON``: Client opens a connection to the cluster in a blocking manner
  by not resolving any of the waiting invocations.
- ``ASYNC``: Client opens a connection to the cluster in a non-blocking
  manner by resolving all the waiting invocations with
  ``ClientOfflineError``.

Its default value is ``ON``.

The example configuration below show how to configure a Python client’s
starting and reconnecting modes.

.. code:: python

    from hazelcast.config import ReconnectMode

    client = hazelcast.HazelcastClient(
        async_start=False,
        # You can also set this to "ON"
        # without importing anything.
        reconnect_mode=ReconnectMode.ON
    )

Configuring Client Connection Retry
-----------------------------------

When the client is disconnected from the cluster or trying to connect
to a one for the first time, it searches for new connections. You can
configure the frequency of the connection attempts and the client
shutdown behavior using the arguments below.

.. code:: python

    client = hazelcast.HazelcastClient(
        retry_initial_backoff=1,
        retry_max_backoff=15,
        retry_multiplier=1.5,
        retry_jitter=0.2,
        cluster_connect_timeout=120
    )

The following are configuration element descriptions:

- ``retry_initial_backoff``: Specifies how long to wait (backoff), in
  seconds, after the first failure before retrying. Its default value
  is ``1``. It must be non-negative.
- ``retry_max_backoff``: Specifies the upper limit for the backoff in
  seconds. Its default value is ``30``. It must be non-negative.
- ``retry_multiplier``: Factor to multiply the backoff after a failed
  retry. Its default value is ``1.05``. It must be greater than or equal
  to ``1``.
- ``retry_jitter``: Specifies by how much to randomize backoffs. Its
  default value is ``0``. It must be in range ``0`` to ``1``.
- ``cluster_connect_timeout``: Timeout value in seconds for the client
  to give up to connect to the current cluster. Its default value is
  ``-1``. For the default value, client will not stop trying to connect
  to the target cluster. (infinite timeout)

A pseudo-code is as follows:

.. code:: text

    begin_time = get_current_time()
    current_backoff = INITIAL_BACKOFF
    while (try_connect(connection_timeout)) != SUCCESS) {
        if (get_current_time() - begin_time >= CLUSTER_CONNECT_TIMEOUT) {
            // Give up to connecting to the current cluster and switch to another if exists.
            // For the default values, CLUSTER_CONNECT_TIMEOUT is infinite.
        }
        sleep(current_backoff + uniform_random(-JITTER * current_backoff, JITTER * current_backoff))
        current_backoff = min(current_backoff * MULTIPLIER, MAX_BACKOFF)
    }

Note that, ``try_connect`` above tries to connect to any member that the
client knows, and for each connection we have a connection timeout; see
the :ref:`setting_up_client_network:setting connection timeout`
section.