Securing Client Connection
==========================

This chapter describes the security features of Hazelcast Python client.
These include using TLS/SSL for connections between members and between
clients and members, mutual authentication, username/password authentication,
token authentication and Kerberos authentication. These security features
require **Hazelcast Enterprise** edition.

TLS/SSL
-------

One of the offers of Hazelcast is the TLS/SSL protocol which you can use
to establish an encrypted communication across your cluster with key
stores and trust stores.

- A Java ``keyStore`` is a file that includes a private key and a
  public certificate. The equivalent of a key store is the combination
  of ``keyfile`` and ``certfile`` at the Python client side.

- A Java ``trustStore`` is a file that includes a list of certificates
  trusted by your application which is named certificate authority. The
  equivalent of a trust store is a ``cafile`` at the Python client
  side.

You should set ``keyStore`` and ``trustStore`` before starting the
members. See the next section on how to set ``keyStore`` and
``trustStore`` on the server side.

TLS/SSL for Hazelcast Members
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Hazelcast allows you to encrypt socket level communication between
Hazelcast members and between Hazelcast clients and members, for end to
end encryption. To use it, see the `TLS/SSL for Hazelcast Members section
<https://docs.hazelcast.com/hazelcast/latest/security/tls-ssl.html#tlsssl-for-hazelcast-members>`__.

TLS/SSL for Hazelcast Python Clients
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

TLS/SSL for the Hazelcast Python client can be configured using the
``SSLConfig`` class. Let’s first give an example of a sample
configuration and then go over the configuration options one by one:

.. code:: python

    from hazelcast.config import SSLProtocol

    client = hazelcast.HazelcastClient(
        ssl_enabled=True,
        ssl_cafile="/home/hazelcast/cafile.pem",
        ssl_certfile="/home/hazelcast/certfile.pem",
        ssl_keyfile="/home/hazelcast/keyfile.pem",
        ssl_password="keyfile-password",
        # You can also set this to "TLSv1_3"
        # without importing anything.
        ssl_protocol=SSLProtocol.TLSv1_3,
        ssl_ciphers="DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA"
    )

Enabling TLS/SSL
^^^^^^^^^^^^^^^^

TLS/SSL for the Hazelcast Python client can be enabled/disabled using
the ``ssl_enabled`` option. When this option is set to ``True``, TLS/SSL
will be configured with respect to the other SSL options. Setting this
option to ``False`` will result in discarding the other SSL options.

The following is an example configuration:

.. code:: python

    client = hazelcast.HazelcastClient(
        ssl_enabled=True
    )

Default value is ``False`` (disabled).

Setting CA File
^^^^^^^^^^^^^^^

Certificates of the Hazelcast members can be validated against
``ssl_cafile``. This option should point to the absolute path of the
concatenated CA certificates in PEM format. When SSL is enabled and
``ssl_cafile`` is not set, a set of default CA certificates from default
locations will be used.

The following is an example configuration:

.. code:: python

    client = hazelcast.HazelcastClient(
        ssl_cafile="/home/hazelcast/cafile.pem"
    )

Setting Client Certificate
^^^^^^^^^^^^^^^^^^^^^^^^^^

When mutual authentication is enabled on the member side, clients or
other members should also provide a certificate file that identifies
themselves. Then, Hazelcast members can use these certificates to
validate the identity of their peers.

Client certificate can be set using the ``ssl_certfile``. This option
should point to the absolute path of the client certificate in PEM
format.

The following is an example configuration:

.. code:: python

    client = hazelcast.HazelcastClient(
        ssl_certfile="/home/hazelcast/certfile.pem"
    )

Setting Private Key
^^^^^^^^^^^^^^^^^^^

Private key of the ``ssl_certfile`` can be set using the
``ssl_keyfile``. This option should point to the absolute path of the
private key file for the client certificate in the PEM format.

If this option is not set, private key will be taken from
``ssl_certfile``. In this case, ``ssl_certfile`` should be in the
following format.

::

    -----BEGIN RSA PRIVATE KEY-----
    ... (private key in base64 encoding) ...
    -----END RSA PRIVATE KEY-----
    -----BEGIN CERTIFICATE-----
    ... (certificate in base64 PEM encoding) ...
    -----END CERTIFICATE-----

The following is an example configuration:

.. code:: python

    client = hazelcast.HazelcastClient(
        ssl_keyfile="/home/hazelcast/keyfile.pem"
    )

Setting Password of the Private Key
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the private key is encrypted using a password, ``ssl_password`` will
be used to decrypt it. The ``ssl_password`` may be a function to call to
get the password. In that case, it will be called with no arguments, and
it should return a string, bytes or bytearray. If the return value is a
string it will be encoded as UTF-8 before using it to decrypt the key.

Alternatively a string, ``bytes`` or ``bytearray`` value may be supplied
directly as the password.

The following is an example configuration:

.. code:: python

    client = hazelcast.HazelcastClient(
        ssl_password="keyfile-password"
    )

Setting the Protocol
^^^^^^^^^^^^^^^^^^^^

``ssl_protocol`` can be used to select the protocol that will be used in
the TLS/SSL communication. Hazelcast Python client offers the following
protocols:

- **SSLv2** : SSL 2.0 Protocol. *RFC 6176 prohibits the usage of SSL
  2.0.*
- **SSLv3** : SSL 3.0 Protocol. *RFC 7568 prohibits the usage of SSL
  3.0.*
- **TLSv1** : TLS 1.0 Protocol described in RFC 2246
- **TLSv1_1** : TLS 1.1 Protocol described in RFC 4346
- **TLSv1_2** : TLS 1.2 Protocol described in RFC 5246
- **TLSv1_3** : TLS 1.3 Protocol described in RFC 8446

..

    Note that TLSv1_3 requires at least Python 3.7 built with
    OpenSSL 1.1.1+.

These protocol versions can be selected using the ``ssl_protocol`` as
follows:

.. code:: python

    from hazelcast.config import SSLProtocol

    client = hazelcast.HazelcastClient(
        ssl_protocol=SSLProtocol.TLSv1_3
    )

..

    Note that the Hazelcast Python client and the Hazelcast members
    should have the same protocol version in order for TLS/SSL to work.
    In case of the protocol mismatch, connection attempts will be
    refused.

Default value is ``SSLProtocol.TLSv1_2``.

Setting Cipher Suites
^^^^^^^^^^^^^^^^^^^^^

Cipher suites that will be used in the TLS/SSL communication can be set
using the ``ssl_ciphers`` option. Cipher suites should be in the OpenSSL
cipher list format. More than one cipher suite can be set by separating
them with a colon.

TLS/SSL implementation will honor the cipher suite order. So, Hazelcast
Python client will offer the ciphers to the Hazelcast members with the
given order.

Note that, when this option is not set, all the available ciphers will
be offered to the Hazelcast members with their default order.

The following is an example configuration:

.. code:: python

    client = hazelcast.HazelcastClient(
        ssl_ciphers="DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA"
    )

Mutual Authentication
~~~~~~~~~~~~~~~~~~~~~

As explained above, Hazelcast members have key stores used to identify
themselves (to other members) and Hazelcast clients have trust stores
used to define which members they can trust.

Using mutual authentication, the clients also have their key stores and
members have their trust stores so that the members can know which
clients they can trust.

To enable mutual authentication, firstly, you need to set the following
property on the server side in the ``hazelcast.xml`` file:

.. code:: xml

    <network>
        <ssl enabled="true">
            <properties>
                <property name="javax.net.ssl.mutualAuthentication">REQUIRED</property>
            </properties>
        </ssl>
    </network>

You can see the details of setting mutual authentication on the server
side in the `Mutual Authentication
section <https://docs.hazelcast.com/hazelcast/latest/security/tls-ssl.html#mutual-authentication>`__
of the Hazelcast Reference Manual.

On the client side, you have to provide ``ssl_cafile``, ``ssl_certfile``
and ``ssl_keyfile`` on top of the other TLS/SSL configurations. See the
:ref:`securing_client_connection:tls/ssl for hazelcast python clients`
section for the details of these options.

Username/Password Authentication
--------------------------------

You can protect your cluster using a username and password pair.
In order to use it, enable it in member configuration:

.. code:: xml

    <security enabled="true">
        <member-authentication realm="passwordRealm"/>
        <realms>
            <realm name="passwordRealm">
                 <identity>
                    <username-password username="MY-USERNAME" password="MY-PASSWORD" />
                </identity>
            </realm>
        </realms>
    </security>

Then, on the client-side, set ``creds_username`` and ``creds_password`` in the configuration:

.. code:: python

    client = hazelcast.HazelcastClient(
        creds_username="MY-USERNAME",
        creds_password="MY-PASSWORD"
    )

Check out the documentation on `Password Credentials
<https://docs.hazelcast.com/hazelcast/latest/security/security-realms.html#password-credentials>`__
of the Hazelcast Documentation.

Token-Based Authentication
--------------------------

Python client supports token-based authentication via token providers.
A token provider is a class derived from :class:`hazelcast.security.TokenProvider`.

In order to use token based authentication, first define in the member configuration:

.. code:: xml

    <security enabled="true">
        <member-authentication realm="tokenRealm"/>
        <realms>
            <realm name="tokenRealm">
                 <identity>
                    <token>MY-SECRET</token>
                </identity>
            </realm>
        </realms>
    </security>

Using :class:`hazelcast.security.BasicTokenProvider` you can pass the given token the member:

.. code:: python

    token_provider = BasicTokenProvider("MY-SECRET")
    client = hazelcast.HazelcastClient(
        token_provider=token_provider
    )

Kerberos Authentication
-----------------------

Python client supports Kerberos authentication with an external package.
The package provides the necessary token provider that handles the
authentication against the KDC (key distribution center) with the given
credentials, receives and caches the ticket, and finally retrieves the token.

For more information and possible client and server configurations, refer to
the `documentation <https://github.com/hazelcast/hazelcast-python-client-kerberos>`__ of the
``hazelcast-kerberos`` package.
