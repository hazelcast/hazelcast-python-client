"""
User API for Serialization.
"""


class ObjectDataOutput(object):
    """ObjectDataOutput provides an interface to convert any of primitive types or arrays of them to series of bytes and
    write them on a stream.
    """

    def write_from(self, buff, offset=None, length=None):
        """Writes the content of the buffer to this output stream.

        Args:
            buff (bytearray): Input buffer.
            offset (int): Offset of the buffer where copy begin.
            length (int): Length of data to be copied from the offset into stream.
        """
        raise NotImplementedError()

    def write_boolean(self, val):
        """Writes a bool value to this output stream.

        Single byte value 1 represent True, 0 represent False

        Args:
            val (bool): The bool to be written.
        """
        raise NotImplementedError()

    def write_byte(self, val):
        """Writes a byte value to this output stream.

        Args:
            val (int): The byte value to be written.
        """
        raise NotImplementedError()

    def write_short(self, val):
        """Writes a short value to this output stream.

        Args:
            val (int): The short value to be written.
        """
        raise NotImplementedError()

    def write_char(self, val):
        """Writes a char value to this output stream.

        Args:
            val (str): The char value to be written.
        """
        raise NotImplementedError()

    def write_int(self, val):
        """Writes a int value to this output stream.

        Args:
            val (int): The int value to be written.
        """
        raise NotImplementedError()

    def write_long(self, val):
        """Writes a long value to this output stream.

        Args:
            val (int): The long value to be written.
        """
        raise NotImplementedError()

    def write_float(self, val):
        """Writes a float value to this output stream.

        Args:
            val (float): The float value to be written.
        """
        raise NotImplementedError()

    def write_double(self, val):
        """Writes a double value to this output stream.

        Args:
            val (float): The double value to be written.
        """
        raise NotImplementedError()

    def write_bytes(self, val):
        """Writes a string to this output stream.

        Args:
            val (str): The string to be written.
        """
        raise NotImplementedError()

    def write_chars(self, val):
        """Writes every character of string to this output stream.

        Args:
            val (str): The string to be written.
        """
        raise NotImplementedError()

    def write_utf(self, val):
        """Writes UTF-8 string to this output stream.

        Args:
            val (str): The UTF-8 string to be written.
        """
        raise NotImplementedError()

    def write_byte_array(self, val):
        """Writes a byte array to this output stream.

        Args:
            val (bytearray): The byte array to be written.
        """
        raise NotImplementedError()

    def write_boolean_array(self, val):
        """Writes a bool array to this output stream.

        Args:
            val (list[bool]): The bool array to be written.
        """
        raise NotImplementedError()

    def write_char_array(self, val):
        """Writes a char array to this output stream.

        Args:
            val  (list[str]): The char array to be written.
        """
        raise NotImplementedError()

    def write_int_array(self, val):
        """Writes a int array to this output stream.

        Args:
            val (list[int]): The int array to be written.
        """
        raise NotImplementedError()

    def write_long_array(self, val):
        """Writes a long array to this output stream.

        Args:
            val (list[int]): The long array to be written.
        """
        raise NotImplementedError()

    def write_double_array(self, val):
        """Writes a double array to this output stream.

        Args:
            val (list[float]): The double array to be written.
        """
        raise NotImplementedError()

    def write_float_array(self, val):
        """Writes a float array to this output stream.

        Args:
            val (list[float]): The float array to be written.
        """
        raise NotImplementedError()

    def write_short_array(self, val):
        """Writes a short array to this output stream.

        Args:
            val (list[int]): The short array to be written.
        """
        raise NotImplementedError()

    def write_utf_array(self, val):
        """Writes a UTF-8 String array to this output stream.

        Args:
            val (list[str]): The UTF-8 String array to be written.
        """
        raise NotImplementedError()

    def write_object(self, val):
        """Writes an object to this output stream.

        Args:
            val: The object to be written.
        """
        raise NotImplementedError()

    def to_byte_array(self):
        """Returns a copy of internal byte array.

        Returns:
            bytearray: The copy of internal byte array
        """
        raise NotImplementedError()

    def get_byte_order(self):
        """Returns the order of bytes, as BIG_ENDIAN or LITTLE_ENDIAN.

        Returns:
            str:
        """
        raise NotImplementedError()


class ObjectDataInput(object):
    """ObjectDataInput provides an interface to read bytes from a stream and reconstruct it to any of primitive types or
    arrays of them.
    """

    def read_into(self, buff, offset=None, length=None):
        """Reads the content of the buffer into an array of bytes.

        Args:
            buff (bytearray): Input buffer.
            offset (int): Offset of the buffer where the read begin.
            length (int): Length of data to be read.

        Returns:
            bytearray: The read data.
        """
        raise NotImplementedError()

    def skip_bytes(self, count):
        """Skips over given number of bytes from input stream.

        Args:
            count (int): Number of bytes to be skipped.

        Returns:
            int: The actual number of bytes skipped.
        """
        raise NotImplementedError()

    def read_boolean(self):
        """Reads 1 byte from input stream and convert it to a bool value.

        Returns:
            bool: The bool value read.
        """
        raise NotImplementedError()

    def read_byte(self):
        """Reads 1 byte from input stream and returns it.

        Returns:
            int: The byte value read.
        """
        raise NotImplementedError()

    def read_unsigned_byte(self):
        """Reads 1 byte from input stream, zero-extends it and returns.

        Returns:
            int: The unsigned byte value read.
        """
        raise NotImplementedError()

    def read_short(self):
        """Reads 2 bytes from input stream and returns a short value.

        Returns:
            int: The short value read.
        """
        raise NotImplementedError()

    def read_unsigned_short(self):
        """Reads 2 bytes from input stream and returns an int value.

        Returns:
            int: The unsigned short value read.
        """
        raise NotImplementedError()

    def read_char(self):
        """Reads 2 bytes from the input stream and returns a str value.

        Returns:
            str: The char value read.
        """
        raise NotImplementedError()

    def read_int(self):
        """Reads 4 bytes from input stream and returns an int value.

        Returns:
            int: The int value read.
        """
        raise NotImplementedError()

    def read_long(self):
        """Reads 8 bytes from input stream and returns a long value.

        Returns:
            int: The int value read.
        """
        raise NotImplementedError()

    def read_float(self):
        """Reads 4 bytes from input stream and returns a float value.

        Returns:
            float: The float value read.
        """
        raise NotImplementedError()

    def read_double(self):
        """Reads 8 bytes from input stream and returns a double value.

        Returns:
            float: The double value read.
        """
        raise NotImplementedError()

    def read_utf(self):
        """Reads a UTF-8 string from input stream and returns it.

        Returns:
            str: The UTF-8 string read.
        """
        raise NotImplementedError()

    def read_byte_array(self):
        """Reads a byte array from input stream and returns it.

        Returns:
            bytearray: The byte array read.
        """
        raise NotImplementedError()

    def read_boolean_array(self):
        """Reads a bool array from input stream and returns it.

        Returns:
            list[bool]: The bool array read.
        """
        raise NotImplementedError()

    def read_char_array(self):
        """Reads a char array from input stream and returns it.

        Returns:
            list[str]: The char array read.
        """
        raise NotImplementedError()

    def read_int_array(self):
        """Reads a int array from input stream and returns it.

        Returns:
            list[int]: The int array read.
        """
        raise NotImplementedError()

    def read_long_array(self):
        """Reads a long array from input stream and returns it.

        Returns:
            list[int]: The long array read.
        """
        raise NotImplementedError()

    def read_double_array(self):
        """Reads a double array from input stream and returns it.

        Returns:
            list[float]: The double array read.
        """
        raise NotImplementedError()

    def read_float_array(self):
        """Reads a float array from input stream and returns it.

        Returns:
            list[float]: The float array read.
        """
        raise NotImplementedError()

    def read_short_array(self):
        """Reads a short array from input stream and returns it.

        Returns:
            list[int]: The short array read.
        """
        raise NotImplementedError()

    def read_utf_array(self):
        """Reads a UTF-8 string array from input stream and returns it.

        Returns:
            list[str]: The UTF-8 string array read.
        """
        raise NotImplementedError()

    def read_object(self):
        """Reads a object from input stream and returns it.

        Returns:
            The object read.
        """
        raise NotImplementedError()

    def get_byte_order(self):
        """Returns the order of bytes, as BIG_ENDIAN or LITTLE_ENDIAN.

        Returns:
            str:
        """
        raise NotImplementedError()

    def position(self):
        """Returns current position in buffer.

        Returns:
            int: Current position in buffer.
        """
        raise NotImplementedError()

    def size(self):
        """Returns size of buffer.

        Returns:
            int: size of buffer.
        """
        raise NotImplementedError()


class IdentifiedDataSerializable(object):
    """IdentifiedDataSerializable is an alternative serialization method to Python pickle, which also avoids reflection
    during de-serialization.

    Each IdentifiedDataSerializable is created by a registered DataSerializableFactory.
    """

    def write_data(self, object_data_output):
        """Writes object fields to output stream.

        Args:
            object_data_output (hazelcast.serialization.api.ObjectDataOutput): The output.
        """
        raise NotImplementedError(
            "read_data must be implemented to serialize this IdentifiedDataSerializable"
        )

    def read_data(self, object_data_input):
        """Reads fields from the input stream.

        Args:
            object_data_input (hazelcast.serialization.api.ObjectDataInput): The input.
        """
        raise NotImplementedError(
            "read_data must be implemented to deserialize this IdentifiedDataSerializable"
        )

    def get_factory_id(self):
        """Returns DataSerializableFactory factory id for this class.

        Returns:
            int: The factory id.
        """
        raise NotImplementedError(
            "This method must return the factory ID for this IdentifiedDataSerializable"
        )

    def get_class_id(self):
        """Returns type identifier for this class. Id should be unique per DataSerializableFactory.

        Returns:
            int: The type id.
        """
        raise NotImplementedError(
            "This method must return the class ID for this IdentifiedDataSerializable"
        )


class Portable(object):
    """Portable provides an alternative serialization method.

    Instead of relying on reflection, each Portable is created by a registered PortableFactory.
    Portable serialization has the following advantages:

    - Support multiversion of the same object type.
    - Fetching individual fields without having to rely on reflection.
    - Querying and indexing support without de-serialization and/or reflection.
    """

    def write_portable(self, writer):
        """Serialize this portable object using given PortableWriter.

        Args:
            writer (hazelcast.serialization.api.PortableWriter): The PortableWriter.
        """
        raise NotImplementedError()

    def read_portable(self, reader):
        """Read portable fields using PortableReader.

        Args:
            reader (hazelcast.serialization.api.PortableReader): The PortableReader.
        """
        raise NotImplementedError()

    def get_factory_id(self):
        """Returns PortableFactory id for this portable class

        Returns:
            int: The factory id.
        """
        raise NotImplementedError()

    def get_class_id(self):
        """Returns class identifier for this portable class. Class id should be unique per PortableFactory.

        Returns:
            int: The class id.
        """
        raise NotImplementedError()


class StreamSerializer(object):
    """A base class for custom serialization."""

    def write(self, out, obj):
        """Writes object to ObjectDataOutput

        Args:
            out (hazelcast.serialization.api.ObjectDataOutput): Stream that object will be written to.
            obj: The object to be written.
        """
        raise NotImplementedError("write method must be implemented")

    def read(self, inp):
        """Reads object from objectDataInputStream

        Args:
            inp (hazelcast.serialization.api.ObjectDataInput): Stream that object will read from.

        Returns:
            The read object.
        """
        raise NotImplementedError("write method must be implemented")

    def get_type_id(self):
        """Returns typeId of serializer.

        Returns:
            int: The type id of the serializer.
        """
        raise NotImplementedError("get_type_id must be implemented")

    def destroy(self):
        """Called when instance is shutting down.

        It can be used to clear used resources.
        """
        raise NotImplementedError()


class PortableReader(object):
    """Provides a mean of reading portable fields from binary in form of Python primitives and arrays of these
    primitives, nested portable fields and array of portable fields.
    """

    def get_version(self):
        """Returns the global version of portable classes.

        Returns:
            int: Global version of portable classes.
        """
        raise NotImplementedError()

    def has_field(self, field_name):
        """Determine whether the field name exists in this portable class or not.

        Args:
            field_name (str): name of the field (does not support nested paths).

        Returns:
            bool: ``True`` if the field name exists in class, ``False`` otherwise.
        """
        raise NotImplementedError()

    def get_field_names(self):
        """Returns the set of field names on this portable class.

        Returns:
            set: Set of field names on this portable class.
        """
        raise NotImplementedError()

    def get_field_type(self, field_name):
        """Returns the field type of given field name.

        Args:
            field_name (str): Name of the field.

        Returns:
            hazelcast.serialization.portable.classdef.FieldType: The field type.
        """
        raise NotImplementedError()

    def get_field_class_id(self, field_name):
        """Returns the class id of given field.

        Args:
          field_name (str): Name of the field.

        Returns:
            int: class id of given field.
        """
        raise NotImplementedError()

    def read_int(self, field_name):
        """Reads a primitive int.

        Args:
            field_name (str): Name of the field.

        Returns:
            int: The int value read.
        """
        raise NotImplementedError()

    def read_long(self, field_name):
        """Reads a primitive long.

        Args:
            field_name (str): Name of the field.

        Returns:
            int: The long value read.
        """
        raise NotImplementedError()

    def read_utf(self, field_name):
        """Reads a UTF-8 String.

        Args:
            field_name (str): Name of the field.

        Returns:
            str: The UTF-8 String read.
        """
        raise NotImplementedError()

    def read_boolean(self, field_name):
        """Reads a primitive bool.

        Args:
            field_name (str): Name of the field.

        Returns:
            bool: The bool value read.
        """
        raise NotImplementedError()

    def read_byte(self, field_name):
        """Reads a primitive byte.

        Args:
            field_name (str): Name of the field.

        Returns:
            int: The byte value read.
        """
        raise NotImplementedError()

    def read_char(self, field_name):
        """Reads a primitive char.

        Args:
            field_name (str): Name of the field.

        Returns:
            str: The char value read.
        """
        raise NotImplementedError()

    def read_double(self, field_name):
        """Reads a primitive double.

        Args:
            field_name (str): Name of the field.

        Returns:
            float: The double value read.
        """
        raise NotImplementedError()

    def read_float(self, field_name):
        """Reads a primitive float.

        Args:
            field_name (str): Name of the field.

        Returns:
            float: The float value read.
        """
        raise NotImplementedError()

    def read_short(self, field_name):
        """Reads a primitive short.

        Args:
            field_name (str): Name of the field.

        Returns:
            int: The short value read.
        """
        raise NotImplementedError()

    def read_portable(self, field_name):
        """Reads a portable.

        Args:
            field_name (str): Name of the field.

        Returns:
            hazelcast.serialization.api.Portable: The portable read.
        """
        raise NotImplementedError()

    def read_byte_array(self, field_name):
        """Reads a primitive byte array.

        Args:
            field_name (str): Name of the field.

        Returns:
            bytearray: The byte array read.
        """
        raise NotImplementedError()

    def read_boolean_array(self, field_name):
        """Reads a primitive bool array.

        Args:
            field_name (str): Name of the field.

        Returns:
            list[bool]): The bool array read.
        """
        raise NotImplementedError()

    def read_char_array(self, field_name):
        """Reads a primitive char array.

        Args:
            field_name (str): Name of the field.

        Returns:
            list[str]): The char array read.
        """
        raise NotImplementedError()

    def read_int_array(self, field_name):
        """Reads a primitive int array.

        Args:
            field_name (str): Name of the field.

        Returns:
            list[int]: The int array read.
        """
        raise NotImplementedError()

    def read_long_array(self, field_name):
        """Reads a primitive long array.

        Args:
            field_name (str): Name of the field.

        Returns:
            list[int]: The long array read.
        """
        raise NotImplementedError()

    def read_double_array(self, field_name):
        """Reads a primitive double array.

        Args:
            field_name (str): Name of the field.

        Returns:
            list[float]: The double array read.
        """
        raise NotImplementedError()

    def read_float_array(self, field_name):
        """Reads a primitive float array.

        Args:
            field_name (str): Name of the field.

        Returns:
            list[float]: The float array read.
        """
        raise NotImplementedError()

    def read_short_array(self, field_name):
        """Reads a primitive short array.

        Args:
            field_name (str): Name of the field.

        Returns:
            list[int]: The short array read.
        """
        raise NotImplementedError()

    def read_utf_array(self, field_name):
        """Reads a UTF-8 String array.

        Args:
            field_name (str): Name of the field.

        Returns:
            str: The UTF-8 String array read.
        """
        raise NotImplementedError()

    def read_portable_array(self, field_name):
        """Reads a portable array.

        Args:
            field_name (str): Name of the field.

        Returns:
            list[hazelcast.serialization.api.Portable]: The portable array read.
        """
        raise NotImplementedError()

    def get_raw_data_input(self):
        """After reading portable fields, one can read remaining fields in old fashioned way
        consecutively from the end of stream. After get_raw_data_input() called, no data can be read.

        Returns:
            hazelcast.serialization.api.ObjectDataInput: The input.
        """
        raise NotImplementedError()


class PortableWriter(object):
    """Provides a mean of writing portable fields to a binary in form of Python primitives and arrays of these
    primitives, nested portable fields and array of portable fields.
    """

    def write_int(self, field_name, value):
        """Writes a primitive int.

        Args:
            field_name (str): Name of the field.
            value (int): Int value to be written.
        """
        raise NotImplementedError()

    def write_long(self, field_name, value):
        """Writes a primitive long.

        Args:
            field_name (str): Name of the field.
            value (int): Long value to be written.
        """
        raise NotImplementedError()

    def write_utf(self, field_name, value):
        """Writes an UTF string.

        Args:
            field_name (str): Name of the field.
            value (str): UTF string value to be written.
        """
        raise NotImplementedError()

    def write_boolean(self, field_name, value):
        """Writes a primitive bool.

        Args:
            field_name (str): Name of the field.
            value (bool): Bool value to be written.
        """
        raise NotImplementedError()

    def write_byte(self, field_name, value):
        """Writes a primitive byte.

        Args:
            field_name (str): Name of the field.
            value (int): Byte value to be written.
        """
        raise NotImplementedError()

    def write_char(self, field_name, value):
        """Writes a primitive char.

        Args:
            field_name (str): Name of the field.
            value (str): Char value to be written.
        """
        raise NotImplementedError()

    def write_double(self, field_name, value):
        """Writes a primitive double.

        Args:
            field_name (str): Name of the field.
            value (float): Double value to be written.
        """
        raise NotImplementedError()

    def write_float(self, field_name, value):
        """Writes a primitive float.

        Args:
            field_name (str): Name of the field.
            value (float): Float value to be written.
        """
        raise NotImplementedError()

    def write_short(self, field_name, value):
        """Writes a primitive short.

        Args:
            field_name (str): Name of the field.
            value (int): Short value to be written.
        """
        raise NotImplementedError()

    def write_portable(self, field_name, portable):
        """Writes a Portable.

        Args:
            field_name (str): Name of the field.
            portable (hazelcast.serialization.api.Portable): Portable to be written.
        """
        raise NotImplementedError()

    def write_null_portable(self, field_name, factory_id, class_id):
        """To write a null portable value, user needs to provide class and factory ids of related class.

        Args:
            field_name (str): Name of the field.
            factory_id (int): Factory id of related portable class.
            class_id (int): Class id of related portable class.
        """
        raise NotImplementedError()

    def write_byte_array(self, field_name, values):
        """Writes a primitive byte array.

        Args:
            field_name (str): Name of the field.
            values (bytearray): Bytearray to be written.
        """
        raise NotImplementedError()

    def write_boolean_array(self, field_name, values):
        """Writes a primitive bool array.

        Args:
            field_name (str): Name of the field.
            values (list[bool]): Bool array to be written.
        """
        raise NotImplementedError()

    def write_char_array(self, field_name, values):
        """Writes a primitive char array.

        Args:
            field_name (str): Name of the field.
            values (list[str]): Char array to be written.
        """
        raise NotImplementedError()

    def write_int_array(self, field_name, values):
        """Writes a primitive int array.

        Args:
            field_name (str): Name of the field.
            values (list[int]): Int array to be written.
        """
        raise NotImplementedError()

    def write_long_array(self, field_name, values):
        """Writes a primitive long array.

        Args:
            field_name (str): Name of the field.
            values (list[int]): Long array to be written.
        """
        raise NotImplementedError()

    def write_double_array(self, field_name, values):
        """Writes a primitive double array.

        Args:
            field_name (str): Name of the field.
            values (list[float]): Double array to be written.
        """
        raise NotImplementedError()

    def write_float_array(self, field_name, values):
        """Writes a primitive float array.

        Args:
            field_name (str): Name of the field.
            values (list[float]): Float array to be written.
        """
        raise NotImplementedError()

    def write_short_array(self, field_name, values):
        """Writes a primitive short array.

        Args:
            field_name (str): Name of the field.
            values: (list[int]): Short array to be written.
        """
        raise NotImplementedError()

    def write_utf_array(self, field_name, values):
        """Writes a UTF-8 String array.

        Args:
            field_name (str): Name of the field.
            values: (str): UTF-8 String array to be written.
        """
        raise NotImplementedError()

    def write_portable_array(self, field_name, values):
        """Writes a portable array.

        Args:
            field_name (str): Name of the field.
            values (list[hazelcast.serialization.api.Portable]): Portable array to be written.
        """
        raise NotImplementedError()

    def get_raw_data_output(self):
        """After writing portable fields, one can write remaining fields in old fashioned way
        consecutively at the end of stream. After get_raw_data_output() called, no data can be written.

        Returns:
            hazelcast.serialization.api.ObjectDataOutput: The output.
        """
        raise NotImplementedError()
