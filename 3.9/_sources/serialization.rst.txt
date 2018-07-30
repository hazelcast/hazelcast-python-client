Hazelcast Serialization
=======================

Hazelcast Python Client is a binary compatible implementation of `Hazelcast serialization <http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#serialization>`_.

Content:

- Builtin serializers
- :class:`~hazelcast.serialization.api.IdentifiedDataSerializable`
- :class:`~hazelcast.serialization.api.Portable`
- Custom serializers
- Global serializers

**Warning :**
Python client does not support DataSerializable as it's a Java class name dependency. Please use :class:`~hazelcast.serialization.api.IdentifiedDataSerializable` instead.


Builtin serializers
-------------------

For Python builtin types; ``None``, ``bool``, ``int``, ``long``, ``float``, ``str``, arrays of them are supported by default.

Integer Type: Python has a different int type than Java. Python 2.x has integer type based on the data size which is int or long.
So in Python we have a configuration for int size to map server data type.

You can configure the default integer type as:

.. code-block:: python

    config.serialization_config.default_integer_type = INTEGER_TYPE.INT

Please see ::const:`~hazelcast.config.INTEGER_TYPE` for details. Please be careful with INTEGER_TYPE.VAR as static type lang's cannot deserialize it.

There exist an internal serializer for any custom class that is not registered a serializer such as IdentifiedDataSerializable, Portable or Custom Serializer.
So the serializer service searches for a serializer for a custom class; if it fails to find one, it uses Python builtin serialization :mod:`cPickle` to handle it.
Please note that this behaviour can be overridden by a global serializer.

IdentifiedDataSerializable
--------------------------

This is the Python implementation of `IdentifiedDataSerializable <http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#identifieddataserializable>`_ .
In order to implement IdentifiedDataSerializable you can either extend builtin class :class:`~hazelcast.serialization.api.IdentifiedDataSerializable` or
implement your own class with same method signatures as in :class:`~hazelcast.serialization.api.IdentifiedDataSerializable`.

example :class:`~hazelcast.serialization.api.IdentifiedDataSerializable` implementation:

.. code-block:: python


    class Customer(IdentifiedDataSerializable):
        CLASS_ID = 1
        FACTORY_ID = 10
        def __init__(self, customer_id=None, name=None, surname=None):
            self.customer_id = customer_id
            self.name = name
            self.surname = surname

        def write_data(self, out):
            out.write_int(self.customer_id)
            out.write_boolean(self.name)
            out.write_char(self.surname)

        def read_data(self, inp):
            self.customer_id = inp.read_int()
            self.name = inp.read_utf()
            self.surname = inp.read_utf()

        def get_factory_id(self):
            return FACTORY_ID

        def get_class_id(self):
            return CLASS_ID


A factory definition is registered to configuration in order to use the above ``Customer`` class:

.. code-block:: python

    identifiedDataSerializable_factory = {Customer.CLASS_ID: Customer}
    config.serialization_config.add_data_serializable_factory(Customer.FACTORY_ID, identifiedDataSerializable_factory)

With the above registration you can use your class:

.. code-block:: python

    client = HazelcastClient(config)
    map = client.get_map("customer_map")
    #let's assume we have variables customer_id, name, surname
    map.put(customer_id, Customer(customer_id, name, surname))


Portable
--------
This is the Python implementation of `Portable <http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#implementing-portable-serialization>`_ .
In order to implement Portable you can either extend builtin class :class:`~hazelcast.serialization.api.Portable` or
implement your own class with same method signatures as in :class:`~hazelcast.serialization.api.Portable`.


example :class:`~hazelcast.serialization.api.Portable` implementation:

.. code-block:: python

    class Customer(Portable):
        CLASS_ID = 1
        FACTORY_ID = 10
        def __init__(self, customer_id=None, name=None, surname=None):
            self.customer_id = customer_id
            self.name = name
            self.surname = surname

        def write_portable(self, writer):
            writer.write_int("customer_id", self.customer_id)
            writer.write_utf("name", self.name)
            writer.write_utf("surname", self.surname)

        def read_portable(self, reader):
            self.customer_id = reader.read_int("customer_id")
            self.name = reader.read_utf("name")
            self.surname = reader.read_utf("surname")

        def get_factory_id(self):
            return FACTORY_ID

        def get_class_id(self):
            return CLASS_ID

A factory definition is registered to the configuration in order to use the above ``Customer`` class:

.. code-block:: python

    portable_factory = {Customer.CLASS_ID: Customer}
    config.serialization_config.add_portable_factory(Customer.FACTORY_ID, portable_factory)

With the above registration you can use your class:

.. code-block:: python

    client = HazelcastClient(config)
    map = client.get_map("customer_map")
    #let's assume we have variables customer_id, name, surname
    map.put(customer_id, Customer(customer_id, name, surname))

With Portable serialization you can make query on your domain object without having the classes registered on server side:

.. code-block:: python

    from hazelcast.serialization.predicate import sql

    map = client.get_map("customer_map").blocking()
    map.put(1, Customer(1, "John", "Doe"))
    map.put(2, Customer(2, "Jane", "Roe"))

    map.values(sql("name = 'John'"))

Portable serialization supports server side field extraction so you can query an object with many fields without deserializing them

Custom Serializers
------------------

When you need more control of the serialization or want to integrate a third party serialization framework, custom serializers is your way to go.
You simply implement :class:`~hazelcast.serialization.api.StreamSerializer` and register it in the configuration

.. code-block:: python

    #CustomSerializer is already implemented elsewhere and you have a CustomClass
    config.set_custom_serializer(CustomClass, CustomSerializer)

With the above registration, all of the serialization will be delegated to your CustomSerializer.

Please refer to `Hazelcast Custom Serialization <http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#custom-serialization>`_  for details.


Global Serializers
------------------

Please refer to `Hazelcast Global Serializer <http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#global-serializer>`_  for details.