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
- SSL/TLS configuration via :class:`~hazelcast.config.SSLConfig`
- Hazelcast.Cloud configuration via :class:`~hazelcast.config.ClientCloudConfig`

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

SSL/TLS Configuration
---------------------

SSL/TLS can be configured using :class:`~hazelcast.config.SSLConfig` to allow encrypted socket level communication
between Hazelcast members and Hazelcast Python Client. Please note that, Hazelcast members should be started with
SSL enabled to use this feature.

To use SSL to authenticate the Hazelcast members, SSL should be enabled on the client side. Client should also
provide a CA file in the PEM format that includes the certificates offered by Hazelcast members during the handshake.
You should provide the absolute path of your CA file to the cafile field. When SSL is enabled and cafile is not set,
a set of default CA certificates from default locations will be used.

.. code-block:: python

    config.network_config.ssl_config.enabled = True
    config.network_config.ssl_config.cafile = "server.pem"

SSL/TLS with mutual authentication can also be configured using :class:`~hazelcast.config.SSLConfig` to allow Hazelcast
members to authenticate Hazelcast Python Client. To do this, you should also provide a certificate file that will be
offered to Hazelcast members during the handshake. This certificate file may contain the private key or private key
may be provided as a separate file. If your private key is encrypted, you should also specify the password of it.
Please note that, certfile and keyfile fields should point to the absolute path of these files.

.. code-block:: python

    config.network_config.ssl.config.enabled = True
    config.network_config.ssl.config.cafile = "server.pem"
    config.network_config.ssl.config.certfile = "client.pem"
    config.network_config.ssl.config.keyfile = "client-key.pem"
    config.network_config.ssl.config.password = "keyfile-password"

Please see API doc of SSL configuration for more options: :class:`~hazelcast.config.SSLConfig`

Hazelcast Cloud Configuration
-----------------------------
Hazelcast client can be configured to connect a cluster running on the Hazelcast.Cloud using
:class:`~hazelcast.config.ClientCloudConfig`. Please note that, in order for
:class:`~hazelcast.config.ClientCloudConfig` to work, SSL/TLS should be enabled.
You should also specify the group name and password of your cluster using :class:`~hazelcast.config.GroupConfig`.

.. code-block:: python

    config.network_config.ssl_config.enabled = True
    config.group_config.name = "name"
    config.group_config.password = "password"
    config.network_config.cloud_config.enabled = True
    config.network_config.cloud_config.discovery_token = "token"
