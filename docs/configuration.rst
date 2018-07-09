Client Configuration
====================

Hazelcast Python Client has a simple configuration. Configuration module :mod:`~hazelcast.config` is all we need.

Configuration actually creates a :class:`~hazelcast.config.ClientConfig` instance to start the client.

Configuration titles:

- Credential setup via :class:`~hazelcast.config.GroupConfig`
- Network configuration via :class:`~hazelcast.config.ClientNetworkConfig`
- Advanced socket configuration via :class:`~hazelcast.config.SocketOption`
- Serialization configuration via :class:`~hazelcast.config.SerializationConfig`
- Near Cache configuration via :class:`~hazelcast.config.NearCacheConfig`
- SSL configuration via :class:`~hazelcast.config.SSLConfig`

Credential Setup
----------------

:class:`~hazelcast.config.GroupConfig` is used to set group-name/password.

.. code-block:: python

    ClientConfig().group_config.name = "Group name of the Hazelcast cluster"
    ClientConfig().group_config.password = "the password"


Network Configuration
---------------------

In order to connect to the cluster, it's required to setup the network details. The minimal client configuration requires
a list of addresses of the cluster.

.. code-block:: python

    config.network_config.addresses.append('127.0.0.1')
    config.network_config.addresses.append('192.168.1.99')
    config.network_config.addresses.append('the-server:5702')

Another important option is to configure client operation modes as Smart or Dummy.

.. code-block:: python

    config.network_config.smart_routing=True #Smart Mode
    config.network_config.smart_routing=False #Dummy Mode

Please see API doc for details :class:`~hazelcast.config.ClientNetworkConfig`

Serialization Configuration
---------------------------

:class:`~hazelcast.config.SerializationConfig` is used to configure serialization.

Please see :doc:`serialization`


Near Cache Configuration
------------------------

Near cache can be configured using :class:`~hazelcast.config.NearCacheConfig`. For each map there should be a matching
near cache configuration with same name

.. code-block:: python

    config.add_near_cache_config(NearCacheConfig("map-name"))

Please see API doc for near cache configuration options: :class:`~hazelcast.config.NearCacheConfig`

SSL Configuration
-----------------

TLS/SSL can be configured using :class:`~hazelcast.config.SSLConfig`.

.. code-block:: python

    config.network_config.ssl_config.enable = True
    config.network_config.ssl_config.cafile = "server.pem"

Please see API doc for SSL configuration options: :class:`~hazelcast.config.SSLConfig`
