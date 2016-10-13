"""
User API for Serialization.
"""


class ObjectDataOutput(object):
    """
    ObjectDataOutput provides an interface to convert any of primitive types or arrays of them to series of bytes and
    write them on a stream.
    """
    def write_from(self, buff, offset=None, length=None):
        """
        Writes the content of the buffer to this output stream.

        :param buff: input buffer.
        :param offset: (int), offset of the buffer where copy begin (optional).
        :param length: (int), length of data to be copied from the offset into stream (optional).
        """
        raise NotImplementedError()

    def write_boolean(self, val):
        """
        Writes a bool value to this output stream. Single byte value 1 represent True, 0 represent False

        :param val: (bool), the bool to be written.
        """
        raise NotImplementedError()

    def write_byte(self, val):
        """
        Writes a byte value to this output stream.

        :param val: (byte), the byte value to be written.
        """
        raise NotImplementedError()

    def write_short(self, val):
        """
        Writes a short value to this output stream.

        :param val: (short), the short value to be written.
        """
        raise NotImplementedError()

    def write_char(self, val):
        """
        Writes a char value to this output stream.

        :param val: (char), the char value to be written.
        """
        raise NotImplementedError()

    def write_int(self, val):
        """
        Writes a int value to this output stream.

        :param val: (int), the int value to be written.
        """
        raise NotImplementedError()

    def write_long(self, val):
        """
        Writes a long value to this output stream.

        :param val: (long), the long value to be written.
        """
        raise NotImplementedError()

    def write_float(self, val):
        """
        Writes a float value to this output stream.

        :param val: (float), the float value to be written.
        """
        raise NotImplementedError()

    def write_double(self, val):
        """
        Writes a double value to this output stream.

        :param val: (double), the double value to be written.
        """
        raise NotImplementedError()

    def write_bytes(self, string):
        """
        Writes a string to this output stream.

        :param val: (str), the string to be written.
        """
        raise NotImplementedError()

    def write_chars(self, val):
        """
        Writes every character of string to this output stream.

        :param val: (str), the string to be written.
        """
        raise NotImplementedError()

    def write_utf(self, val):
        """
        Writes 2 bytes of length information and UTF-8 string to this output stream.

        :param val: (UTF-8 str), the UTF-8 string to be written.
        """
        raise NotImplementedError()

    def write_byte_array(self, val):
        """
        Writes a byte array to this output stream.

        :param val: (byte array), the byte array to be written.
        """
        raise NotImplementedError()

    def write_boolean_array(self, val):
        """
        Writes a bool array to this output stream.

        :param val: (bool array), the bool array to be written.
        """
        raise NotImplementedError()

    def write_char_array(self, val):
        """
        Writes a char array to this output stream.

        :param val: (char array), the char array to be written.
        """
        raise NotImplementedError()

    def write_int_array(self, val):
        """
        Writes a int array to this output stream.

        :param val: (int array), the int array to be written.
        """
        raise NotImplementedError()

    def write_long_array(self, val):
        """
        Writes a long array to this output stream.

        :param val: (long array), the long array to be written.
        """
        raise NotImplementedError()

    def write_double_array(self, val):
        """
        Writes a double array to this output stream.

        :param val: (double array), the double array to be written.
        """
        raise NotImplementedError()

    def write_float_array(self, val):
        """
        Writes a float array to this output stream.

        :param val: (float array), the float array to be written.
        """
        raise NotImplementedError()

    def write_short_array(self, val):
        """
        Writes a short array to this output stream.

        :param val: (short array), the short array to be written.
        """
        raise NotImplementedError()

    def write_utf_array(self, val):
        """
        Writes a UTF-8 String array to this output stream.

        :param val: (UTF-8 String array), the UTF-8 String array to be written.
        """
        raise NotImplementedError()

    def write_object(self, val):
        """
        Writes an object to this output stream.

        :param val: (object), the object to be written.
        """
        raise NotImplementedError()

    def write_data(self, val):
        """
        Writes a data to this output stream.

        :param val: (Data), the data to be written.
        """
        raise NotImplementedError()

    def to_byte_array(self):
        """
        Returns a copy of internal byte array.

        :return: (byte array), the copy of internal byte array.
        """
        raise NotImplementedError()

    def get_byte_order(self):
        """
        Returns the order of bytes, as BIG_ENDIAN or LITTLE_ENDIAN.

        :return: the order of bytes, as BIG_ENDIAN or LITTLE_ENDIAN.
        """
        raise NotImplementedError()


class ObjectDataInput(object):
    """
    ObjectDataInput provides an interface to read bytes from a stream and reconstruct it to any of primitive types or
    arrays of them.
    """
    def read_into(self, buff, offset=None, length=None):
        """
        Reads the content of the buffer into an array of bytes.

        :param buff: input buffer.
        :param offset: (int), offset of the buffer where the read begin (optional).
        :param length: (int), length of data to be read (optional).
        :return: (byte array), the read data.
        """
        raise NotImplementedError()

    def skip_bytes(self, count):
        """
        Skips over given number of bytes from input stream.

        :param count: (long), number of bytes to be skipped.
        :return: (long), the actual number of bytes skipped.
        """
        raise NotImplementedError()

    def read_boolean(self):
        """
        Reads 1 byte from input stream and convert it to a bool value.

        :return: (bool), the bool value read.
        """
        raise NotImplementedError()

    def read_byte(self):
        """
        Reads 1 byte from input stream and returns it.

        :return: (byte), the byte value read.
        """
        raise NotImplementedError()

    def read_unsigned_byte(self):
        """
        Reads 1 byte from input stream, zero-extends it and returns.

        :return: (unsigned byte), the unsigned byte value read.
        """
        raise NotImplementedError()

    def read_short(self):
        """
        Reads 2 bytes from input stream and returns a short value.

        :return: (short), the short value read.
        """
        raise NotImplementedError()

    def read_unsigned_short(self):
        """
        Reads 2 bytes from input stream and returns an int value.

        :return: (unsigned short), the unsigned short value read.
        """
        raise NotImplementedError()

    def read_int(self):
        """
        Reads 4 bytes from input stream and returns an int value.

        :return: (int), the int value read.
        """
        raise NotImplementedError()

    def read_long(self):
        """
        Reads 8 bytes from input stream and returns a long value.

        :return: (long), the int value read.
        """
        raise NotImplementedError()

    def read_float(self):
        """
        Reads 4 bytes from input stream and returns a float value.

        :return: (float), the float value read.
        """
        raise NotImplementedError()

    def read_double(self):
        """
        Reads 8 bytes from input stream and returns a double value.

        :return: (long), the double value read.
        """
        raise NotImplementedError()

    def read_utf(self):
        """
        Reads a UTF-8 string from input stream and returns it.

        :return: (UTF-8 str), the UTF-8 string read.
        """
        raise NotImplementedError()

    def read_byte_array(self):
        """
        Reads a byte array from input stream and returns it.

        :return: (byte array), the byte array read.
        """
        raise NotImplementedError()

    def read_boolean_array(self):
        """
        Reads a bool array from input stream and returns it.

        :return: (bool array), the bool array read.
        """
        raise NotImplementedError()

    def read_char_array(self):
        """
        Reads a char array from input stream and returns it.

        :return: (char array), the char array read.
        """
        raise NotImplementedError()

    def read_int_array(self):
        """
        Reads a int array from input stream and returns it.

        :return: (int array), the int array read.
        """
        raise NotImplementedError()

    def read_long_array(self):
        """
        Reads a long array from input stream and returns it.

        :return: (long array), the long array read.
        """
        raise NotImplementedError()

    def read_double_array(self):
        """
        Reads a double array from input stream and returns it.

        :return: (double array), the double array read.
        """
        raise NotImplementedError()

    def read_float_array(self):
        """
        Reads a float array from input stream and returns it.

        :return: (float array), the float array read.
        """
        raise NotImplementedError()

    def read_short_array(self):
        """
        Reads a short array from input stream and returns it.

        :return: (short array), the short array read.
        """
        raise NotImplementedError()

    def read_utf_array(self):
        """
        Reads a UTF-8 String array from input stream and returns it.

        :return: (UTF-8 String  array), the UTF-8 String array read.
        """
        raise NotImplementedError()

    def read_object(self):
        """
        Reads a object from input stream and returns it.

        :return: (object), the object read.
        """
        raise NotImplementedError()

    def read_data(self):
        """
        Reads a data from input stream and returns it.

        :return: (Data), the data read.
        """
        raise NotImplementedError()

    def get_byte_order(self):
        """
        Returns the order of bytes, as BIG_ENDIAN or LITTLE_ENDIAN.

        :return: the order of bytes, as BIG_ENDIAN or LITTLE_ENDIAN.
        """
        raise NotImplementedError()


class IdentifiedDataSerializable(object):
    """
    IdentifiedDataSerializable is an alternative serialization method to Python pickle, which also avoids reflection
    during de-serialization. Each IdentifiedDataSerializable is created by a registered DataSerializableFactory.
    """
    def write_data(self, object_data_output):
        """
        Writes object fields to output stream.

        :param object_data_output: (:class:`hazelcast.serialization.api.ObjectDataOutput`), the output.
        """
        raise NotImplementedError("read_data must be implemented to serialize this IdentifiedDataSerializable")

    def read_data(self, object_data_input):
        """
        Reads fields from the input stream.

        :param object_data_input: (:class:`hazelcast.serialization.api.ObjectDataInput`), the input.
        """
        raise NotImplementedError("read_data must be implemented to deserialize this IdentifiedDataSerializable")

    def get_factory_id(self):
        """
        Returns DataSerializableFactory factory id for this class.

        :return: (int), the factory id.
        """
        raise NotImplementedError("This method must return the factory ID for this IdentifiedDataSerializable")

    def get_class_id(self):
        """
        Returns type identifier for this class. Id should be unique per DataSerializableFactory.

        :return: (int), the type id.
        """
        raise NotImplementedError("This method must return the class ID for this IdentifiedDataSerializable")


class Portable(object):
    """
    Portable provides an alternative serialization method. Instead of relying on reflection, each Portable is created by
    a registered PortableFactory. Portable serialization has the following advantages:

        * Support multiversion of the same object type.
        * Fetching individual fields without having to rely on reflection.
        * Querying and indexing support without de-serialization and/or reflection.
    """
    def write_portable(self, writer):
        """
        Serialize this portable object using given PortableWriter.

        :param writer: :class:`hazelcast.serialization.api.PortableWriter`, the PortableWriter.
        """
        raise NotImplementedError()

    def read_portable(self, reader):
        """
        Read portable fields using PortableReader.

        :param reader: :class:`hazelcast.serialization.api.PortableReader`, the PortableReader.
        """
        raise NotImplementedError()

    def get_factory_id(self):
        """
        Returns PortableFactory id for this portable class

        :return: (int), the factory id.
        """
        raise NotImplementedError()

    def get_class_id(self):
        """
        Returns class identifier for this portable class. Class id should be unique per PortableFactory.

        :return: (int), the class id.
        """
        raise NotImplementedError()


class StreamSerializer(object):
    """
    A base class for custom serialization. User can register custom serializer like following:
        >>> class CustomSerializer(StreamSerializer):
        >>>     def __init__(self):
        >>>         super(CustomSerializer, self).__init__()
        >>>
        >>>     def read(self, inp):
        >>>         pass
        >>>
        >>>     def write(self, out, obj):
        >>>         pass
        >>>
        >>>     def get_type_id(self):
        >>>         pass
        >>>
        >>>     def destroy(self):
        >>>         pass

    """
    def write(self, out, obj):
        """
        Writes object to ObjectDataOutput

        :param out: (:class:`hazelcast.serialization.api.ObjectDataOutput`), stream that object will be written to.
        :param obj: (object), the object to be written.
        """
        raise NotImplementedError("write method must be implemented")

    def read(self, inp):
        """
        Reads object from objectDataInputStream

        :param inp: (:class:`hazelcast.serialization.api.ObjectDataInput`) stream that object will read from.
        :return: (object), the read object.
        """
        raise NotImplementedError("write method must be implemented")

    def get_type_id(self):
        """
        Returns typeId of serializer.
        :return: (int), typeId of serializer.
        """
        raise NotImplementedError("get_type_id must be implemented")

    def destroy(self):
        """
        Called when instance is shutting down. It can be used to clear used resources.
        """
        raise NotImplementedError()


class PortableReader(object):
    """
    Provides a mean of reading portable fields from binary in form of Python primitives and arrays of these primitives,
    nested portable fields and array of portable fields.
    """
    def get_version(self):
        """
        Returns the global version of portable classes.

        :return: (int), global version of portable classes.
        """
        raise NotImplementedError()

    def has_field(self, field_name):
        """
        Determine whether the field name exists in this portable class or not.

        :param field_name: (str), name of the field (does not support nested paths).
        :return: (bool), ``true`` if the field name exists in class, ``false`` otherwise.
        """
        raise NotImplementedError()

    def get_field_names(self):
        """
        Returns the set of field names on this portable class.

        :return: (Set), set of field names on this portable class.
        """
        raise NotImplementedError()

    def get_field_type(self, field_name):
        """
        Returns the field type of given field name.

        :param field_name: (str), name of the field.
        :return: the field type.
        """
        raise NotImplementedError()

    def get_field_class_id(self, field_name):
        """
        Returns the class id of given field.

        :param field_name: (str), name of the field.
        :return: (int), class id of given field.
        """
        raise NotImplementedError()

    def read_int(self, field_name):
        """
        Reads a primitive int.

        :param field_name: (str), name of the field.
        :return: (int), the int value read.
        """
        raise NotImplementedError()

    def read_long(self, field_name):
        """
        Reads a primitive long.

        :param field_name: (str), name of the field.
        :return: (long), the long value read.
        """
        raise NotImplementedError()

    def read_utf(self, field_name):
        """
        Reads a UTF-8 String.

        :param field_name: (str), name of the field.
        :return: (UTF-8 str), the UTF-8 String read.
        """
        raise NotImplementedError()

    def read_boolean(self, field_name):
        """
        Reads a primitive bool.

        :param field_name: (str), name of the field.
        :return: (bool), the bool value read.
        """
        raise NotImplementedError()

    def read_byte(self, field_name):
        """
        Reads a primitive byte.

        :param field_name: (str), name of the field.
        :return: (byte), the byte value read.
        """
        raise NotImplementedError()

    def read_char(self, field_name):
        """
        Reads a primitive char.

        :param field_name: (str), name of the field.
        :return: (char), the char value read.
        """
        raise NotImplementedError()

    def read_double(self, field_name):
        """
        Reads a primitive double.

        :param field_name: (str), name of the field.
        :return: (double), the double value read.
        """
        raise NotImplementedError()

    def read_float(self, field_name):
        """
        Reads a primitive float.

        :param field_name: (str), name of the field.
        :return: (float), the float value read.
        """
        raise NotImplementedError()

    def read_short(self, field_name):
        """
        Reads a primitive short.

        :param field_name: (str), name of the field.
        :return: (short), the short value read.
        """
        raise NotImplementedError()

    def read_portable(self, field_name):
        """
        Reads a portable.

        :param field_name: (str), name of the field.
        :return: (:class:`hazelcast.serialization.api.Portable`, the portable read.
        """
        raise NotImplementedError()

    def read_byte_array(self, field_name):
        """
        Reads a primitive byte array.

        :param field_name: (str), name of the field.
        :return: (byte array), the byte array read.
        """
        raise NotImplementedError()

    def read_boolean_array(self, field_name):
        """
        Reads a primitive bool array.

        :param field_name: (str), name of the field.
        :return: (bool array), the bool array read.
        """
        raise NotImplementedError()

    def read_char_array(self, field_name):
        """
        Reads a primitive char array.

        :param field_name: (str), name of the field.
        :return: (char array), the char array read.
        """
        raise NotImplementedError()

    def read_int_array(self, field_name):
        """
        Reads a primitive int array.

        :param field_name: (str), name of the field.
        :return: (int array), the int array read.
        """
        raise NotImplementedError()

    def read_long_array(self, field_name):
        """
        Reads a primitive long array.

        :param field_name: (str), name of the field.
        :return: (long array), the long array read.
        """
        raise NotImplementedError()

    def read_double_array(self, field_name):
        """
        Reads a primitive double array.

        :param field_name: (str), name of the field.
        :return: (double array), the double array read.
        """
        raise NotImplementedError()

    def read_float_array(self, field_name):
        """
        Reads a primitive float array.

        :param field_name: (str), name of the field.
        :return: (float array), the float array read.
        """
        raise NotImplementedError()

    def read_short_array(self, field_name):
        """
        Reads a primitive short array.

        :param field_name: (str), name of the field.
        :return: (short array), the short array read.
        """
        raise NotImplementedError()

    def read_utf_array(self, field_name):
        """
        Reads a UTF-8 String array.

        :param field_name: (str), name of the field.
        :return: (UTF-8 String array), the UTF-8 String array read.
        """
        raise NotImplementedError()

    def read_portable_array(self, field_name):
        """
        Reads a portable array.

        :param field_name: (str), name of the field.
        :return: (Portable array), the portable array read.
        """
        raise NotImplementedError()

    def get_raw_data_input(self):
        """
        After reading portable fields, one can read remaining fields in old fashioned way consecutively from the end of
        stream. After get_raw_data_input() called, no data can be read.

        :return: (:class:`hazelcast.serialization.api.ObjectDataInput`), the input.
        """
        raise NotImplementedError()


class PortableWriter(object):
    """
    Provides a mean of writing portable fields to a binary in form of Python primitives and arrays of these primitives,
    nested portable fields and array of portable fields.
    """
    def write_int(self, field_name, value):
        """
        Writes a primitive int.

        :param field_name: (str), name of the field.
        :param value: (int), int value to be written.
        """
        raise NotImplementedError()

    def write_long(self, field_name, value):
        """
        Writes a primitive long.

        :param field_name: (str), name of the field.
        :param value: (long), long value to be written.
        """
        raise NotImplementedError()

    def write_utf(self, field_name, value):
        """
        Writes an UTF string.

        :param field_name: (str), name of the field.
        :param value: (UTF str), UTF string value to be written.
        """
        raise NotImplementedError()

    def write_boolean(self, field_name, value):
        """
        Writes a primitive bool.

        :param field_name: (str), name of the field.
        :param value: (bool), bool value to be written.
        """
        raise NotImplementedError()

    def write_byte(self, field_name, value):
        """
        Writes a primitive byte.

        :param field_name: (str), name of the field.
        :param value: (byte), byte value to be written.
        """
        raise NotImplementedError()

    def write_char(self, field_name, value):
        """
        Writes a primitive char.

        :param field_name: (str), name of the field.
        :param value: (char), char value to be written.
        """
        raise NotImplementedError()

    def write_double(self, field_name, value):
        """
        Writes a primitive double.

        :param field_name: (str), name of the field.
        :param value: (Dobule), double value to be written.
        """
        raise NotImplementedError()

    def write_float(self, field_name, value):
        """
        Writes a primitive float.

        :param field_name: (str), name of the field.
        :param value: (float), float value to be written.
        """
        raise NotImplementedError()

    def write_short(self, field_name, value):
        """
        Writes a primitive short.

        :param field_name: (str), name of the field.
        :param value: (short), short value to be written.
        """
        raise NotImplementedError()

    def write_portable(self, field_name, portable):
        """
        Writes a Portabl

        :param field_name: (str), name of the field.
        :param portable: (:class:`hazelcast.serialization.api.Portable`, portable to be written.
        """
        raise NotImplementedError()

    def write_null_portable(self, field_name, factory_id, class_id):
        """
        To write a null portable value, user needs to provide class and factoryIds of related class.

        :param field_name: (str), name of the field.
        :param factory_id: (int), factory id of related portable class.
        :param class_id: (int), class id of related portable class.
        """
        raise NotImplementedError()

    def write_byte_array(self, field_name, values):
        """
        Writes a primitive byte array.

        :param field_name: (str), name of the field.
        :param values: (byte array), byte array to be written.
        """
        raise NotImplementedError()

    def write_boolean_array(self, field_name, values):
        """
        Writes a primitive bool array.

        :param field_name: (str), name of the field.
        :param values: (bool array), bool array to be written.
        """
        raise NotImplementedError()

    def write_char_array(self, field_name, values):
        """
        Writes a primitive char array.

        :param field_name: (str), name of the field.
        :param values: (char array), char array to be written.
        """
        raise NotImplementedError()

    def write_int_array(self, field_name, values):
        """
        Writes a primitive int array.

        :param field_name: (str), name of the field.
        :param values: (int array), int array to be written.
        """
        raise NotImplementedError()

    def write_long_array(self, field_name, values):
        """
        Writes a primitive long array.

        :param field_name: (str), name of the field.
        :param values: (long array), long array to be written.
        """
        raise NotImplementedError()

    def write_double_array(self, field_name, values):
        """
        Writes a primitive double array.

        :param field_name: (str), name of the field.
        :param values: (double array), double array to be written.
        """
        raise NotImplementedError()

    def write_float_array(self, field_name, values):
        """
        Writes a primitive float array.

        :param field_name: (str), name of the field.
        :param values: (float array), float array to be written.
        """
        raise NotImplementedError()

    def write_short_array(self, field_name, values):
        """
        Writes a primitive short array.

        :param field_name: (str), name of the field.
        :param values: (short array), short array to be written.
        """
        raise NotImplementedError()

    def write_utf_array(self, field_name, values):
        """
        Writes a UTF-8 String array.

        :param field_name: (str), name of the field.
        :param values: (UTF-8 String array), UTF-8 String array to be written.
        """
        raise NotImplementedError()

    def write_portable_array(self, field_name, values):
        """
        Writes a portable array.

        :param field_name: (str), name of the field.
        :param values: (Portable array), portable array to be written.
        """
        raise NotImplementedError()

    def get_raw_data_output(self):
        """
        After writing portable fields, one can write remaining fields in old fashioned way consecutively at the end of
        stream. After get_raw_data_output() called, no data can be written.

        :return: (:class:`hazelcast.serialization.api.ObjectDataOutput`), the output.
        """
        raise NotImplementedError()

