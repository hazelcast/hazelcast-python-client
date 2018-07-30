Distributed Data Structures
===========================

Hazelcast Python Client functions as a simple proxy to Hazelcast distributed data structures using Hazelcast Client Protocol.
Please refer to `Hazelcast Documentation, Section:Distributed Data Structures <http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#distributed-data-structures>`_
for details of these distributed structures.

Hazelcast Python Client supports the following data structures:

Standard utility collections
----------------------------

- :class:`~hazelcast.proxy.map.Map` is a distributed dictionary. It lets you read from and write to a Hazelcast map with methods such as get and put.
- :class:`~hazelcast.proxy.queue.Queue` is a Concurrent, blocking, distributed, observable queue. You can add an item in one member and remove it from another one.
- :class:`~hazelcast.proxy.ringbuffer.Ringbuffer` is implemented for reliable eventing system. It is also a distributed data structure.
- :class:`~hazelcast.proxy.set.Set` is Concurrent, distributed implementation of ``Set``
- :class:`~hazelcast.proxy.list.List` is similar to Hazelcast Set. The only difference is that it allows duplicate elements and preserves their order.
- :class:`~hazelcast.proxy.multi_map.MultiMap` is a specialized Hazelcast map. It is a distributed data structure where you can store multiple values for a single key.
- :class:`~hazelcast.proxy.replicated_map.ReplicatedMap` does not partition data. It does not spread data to different cluster members. Instead, it replicates the data to all members.


Concurrency utilities
---------------------

- :class:`~hazelcast.proxy.lock.Lock` is a distributed implementation of `asyncio.Lock <https://docs.python.org/3/library/asyncio-sync.html>`_.
- :class:`~hazelcast.proxy.semaphore.Semaphore` is a backed-up distributed alternative to the Python `asyncio.Semaphore <https://docs.python.org/3/library/asyncio-sync.html>`_
- :class:`~hazelcast.proxy.atomic_long.AtomicLong` is a redundant and highly available distributed long value which can be updated atomically.
- :class:`~hazelcast.proxy.atomic_reference.AtomicReference` is an atomically updated reference to an object. When you need to deal with a reference in a distributed environment, you can use Hazelcast AtomicReference.
- :class:`~hazelcast.proxy.id_generator.IdGenerator` is used to generate cluster-wide unique identifiers. ID generation occurs almost at the speed of :func:`~hazelcast.proxy.atomic_long.AtomicLong.increment_and_get()`.
- :class:`~hazelcast.proxy.count_down_latch.CountDownLatch`  is a backed-up, distributed, cluster-wide synchronization aid that allows one or more threads to wait until a set of operations being performed in other threads completes


Distributed Events
------------------

You can register for Hazelcast entry events so you will be notified when those events occur. Event Listeners are cluster-wide--when a listener is registered in one member of cluster, it is actually registered for events that originated at any member in the cluster.
When a new member joins, events originated at the new member will also be delivered.
Please refer to `Hazelcast Documentation, Section:Distributed Events <http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#distributed-events>`_.


Distributed Query
-----------------

Please refer to `Hazelcast Documentation, Section:Distributed Events <http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#distributed-query>`_.
