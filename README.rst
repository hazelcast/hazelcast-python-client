Hazelcast Python Client
=======================

.. image:: https://badges.gitter.im/hazelcast/hazelcast-python-client.svg
   :alt: Join the chat at https://gitter.im/hazelcast/hazelcast-python-client
   :target: https://gitter.im/hazelcast/hazelcast-python-client?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

Python client implementation for `Hazelcast <https://github.com/hazelcast/hazelcast>`_, the open source in-memory data grid.

Python client is implemented using the `Hazelcast Open Binary Client Protocol <http://docs.hazelcast.org/docs/HazelcastOpenBinaryClientProtocol-Version1.0-Final.pdf>`_

**This client is a work in progress**

Features :
----------

* Map
* MultiMap
* List
* Set
* Queue
* Topic
* Lock
* Semaphore
* AtomicLong
* AtomicReference
* IdGenerator
* CountDownLatch
* RingBuffer
* ReplicatedMap
* Transactional Map, MultiMap, Queue, List, Set
* Continuous Query(listener with predicate)
* Distributed Executor Service
* Query (Predicates) 
* API configuration
* Smart and Non-Smart Client operation
* Event Listeners
* Lifecycle Service
* Hazelcast serialization for IdentifiedDataSerializable
* Hazelcast serialization for Portable
* Hazelcast serialization, Custom Serializers
* Hazelcast serialization, Global Serializers


Installation
------------

You can install the Hazelcast python client via the following command::

    $ pip install hazelcast-python-client

or::

    $ python setup.py install

Mail Group
----------

Please join the mail group if you are interested in using or developing Hazelcast.

`http://groups.google.com/group/hazelcast <http://groups.google.com/group/hazelcast>`_

License
~~~~~~~

Hazelcast is available under the Apache 2 License. Please see the `Licensing appendix <http://docs.hazelcast.org/docs/latest/manual/html-single/hazelcast-documentation.html#license-questions>`_ for more information.

Copyright
~~~~~~~~~

Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.

Visit `www.hazelcast.com <http://www.hazelcast.com/>`_ for more info.
