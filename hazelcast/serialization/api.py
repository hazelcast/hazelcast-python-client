"""
User API and docs
"""


class ObjectDataOutput(object):
    def write_from(self, buff, offset=None, length=None):
        """
        Writes the content of the buffer to this output stream
        :param buff: input buffer
        :param offset: offset of the buffer where copy begin
        :param length: length of data to be copied from the offset into stream
        """
        raise NotImplementedError()

    def write_boolean(self, val):
        """
        Writes a boolean value to this output stream
        single byte value 1 represent True, 0 represent False
        :param val: the boolean to be written
        """
        raise NotImplementedError()

    def write_byte(self, val):
        raise NotImplementedError()

    def write_short(self, val):
        raise NotImplementedError()

    def write_char(self, val):
        raise NotImplementedError()

    def write_int(self, val):
        raise NotImplementedError()

    def write_long(self, val):
        raise NotImplementedError()

    def write_float(self, val):
        raise NotImplementedError()

    def write_double(self, val):
        raise NotImplementedError()

    def write_bytes(self, string):
        raise NotImplementedError()

    def write_chars(self, val):
        raise NotImplementedError()

    def write_utf(self, val):
        raise NotImplementedError()

    def write_byte_array(self, val):
        raise NotImplementedError()

    def write_boolean_array(self, val):
        raise NotImplementedError()

    def write_char_array(self, val):
        raise NotImplementedError()

    def write_int_array(self, val):
        raise NotImplementedError()

    def write_long_array(self, val):
        raise NotImplementedError()

    def write_double_array(self, val):
        raise NotImplementedError()

    def write_float_array(self, val):
        raise NotImplementedError()

    def write_short_array(self, val):
        raise NotImplementedError()

    def write_utf_array(self, val):
        raise NotImplementedError()

    def write_object(self, val):
        raise NotImplementedError()

    def write_data(self, val):
        raise NotImplementedError()

    def to_byte_array(self):
        raise NotImplementedError()

    def get_byte_order(self):
        raise NotImplementedError()


class ObjectDataInput(object):
    def read_into(self, buff, offset=None, length=None):
        raise NotImplementedError()

    def skip_bytes(self, count):
        raise NotImplementedError()

    def read_boolean(self):
        raise NotImplementedError()

    def read_byte(self):
        raise NotImplementedError()

    def read_unsigned_byte(self):
        raise NotImplementedError()

    def read_short(self):
        raise NotImplementedError()

    def read_unsigned_short(self):
        raise NotImplementedError()

    def read_int(self):
        raise NotImplementedError()

    def read_long(self):
        raise NotImplementedError()

    def read_float(self):
        raise NotImplementedError()

    def read_double(self):
        raise NotImplementedError()

    def read_utf(self):
        raise NotImplementedError()

    def read_byte_array(self):
        raise NotImplementedError()

    def read_boolean_array(self):
        raise NotImplementedError()

    def read_char_array(self):
        raise NotImplementedError()

    def read_int_array(self):
        raise NotImplementedError()

    def read_long_array(self):
        raise NotImplementedError()

    def read_double_array(self):
        raise NotImplementedError()

    def read_float_array(self):
        raise NotImplementedError()

    def read_short_array(self):
        raise NotImplementedError()

    def read_utf_array(self):
        raise NotImplementedError()

    def read_object(self):
        raise NotImplementedError()

    def read_data(self):
        raise NotImplementedError()

    def get_byte_order(self):
        raise NotImplementedError()


class IdentifiedDataSerializable(object):
    def write_data(self, object_data_output):
        raise NotImplementedError("read_data must be implemented to serialize this IdentifiedDataSerializable")

    def read_data(self, object_data_input):
        raise NotImplementedError("read_data must be implemented to deserialize this IdentifiedDataSerializable")

    def get_factory_id(self):
        raise NotImplementedError("This method must return the factory ID for this IdentifiedDataSerializable")

    def get_class_id(self):
        raise NotImplementedError("This method must return the class ID for this IdentifiedDataSerializable")


class Portable(object):
    def write_portable(self, writer):
        raise NotImplementedError()

    def read_portable(self, reader):
        raise NotImplementedError()

    def get_factory_id(self):
        raise NotImplementedError()

    def get_class_id(self):
        raise NotImplementedError()


class StreamSerializer(object):
    def write(self, out, obj):
        raise NotImplementedError("write method must be implemented")

    def read(self, inp):
        raise NotImplementedError("write method must be implemented")

    def get_type_id(self):
        raise NotImplementedError("get_type_id must be implemented")

    def destroy(self):
        raise NotImplementedError()


class PortableReader(object):
    def get_version(self):
        raise NotImplementedError()

    def has_field(self, field_name):
        raise NotImplementedError()

    def get_field_names(self):
        raise NotImplementedError()

    def get_field_type(self, field_name):
        raise NotImplementedError()

    def get_field_class_id(self, field_name):
        raise NotImplementedError()

    def read_int(self, field_name):
        raise NotImplementedError()

    def read_long(self, field_name):
        raise NotImplementedError()

    def read_utf(self, field_name):
        raise NotImplementedError()

    def read_boolean(self, field_name):
        raise NotImplementedError()

    def read_byte(self, field_name):
        raise NotImplementedError()

    def read_char(self, field_name):
        raise NotImplementedError()

    def read_double(self, field_name):
        raise NotImplementedError()

    def read_float(self, field_name):
        raise NotImplementedError()

    def read_short(self, field_name):
        raise NotImplementedError()

    def read_portable(self, field_name):
        raise NotImplementedError()

    def read_byte_array(self, field_name):
        raise NotImplementedError()

    def read_boolean_array(self, field_name):
        raise NotImplementedError()

    def read_char_array(self, field_name):
        raise NotImplementedError()

    def read_int_array(self, field_name):
        raise NotImplementedError()

    def read_long_array(self, field_name):
        raise NotImplementedError()

    def read_double_array(self, field_name):
        raise NotImplementedError()

    def read_float_array(self, field_name):
        raise NotImplementedError()

    def read_short_array(self, field_name):
        raise NotImplementedError()

    def read_utf_array(self, field_name):
        raise NotImplementedError()

    def read_portable_array(self, field_name):
        raise NotImplementedError()

    def get_raw_data_input(self):
        raise NotImplementedError()


class PortableWriter(object):
    def write_int(self, field_name, value):
        raise NotImplementedError()

    def write_long(self, field_name, value):
        raise NotImplementedError()

    def write_utf(self, field_name, value):
        raise NotImplementedError()

    def write_boolean(self, field_name, value):
        raise NotImplementedError()

    def write_byte(self, field_name, value):
        raise NotImplementedError()

    def write_char(self, field_name, value):
        raise NotImplementedError()

    def write_double(self, field_name, value):
        raise NotImplementedError()

    def write_float(self, field_name, value):
        raise NotImplementedError()

    def write_short(self, field_name, value):
        raise NotImplementedError()

    def write_portable(self, field_name, portable):
        raise NotImplementedError()

    def write_null_portable(self, field_name, factory_id, class_id):
        raise NotImplementedError()

    def write_byte_array(self, field_name, values):
        raise NotImplementedError()

    def write_boolean_array(self, field_name, values):
        raise NotImplementedError()

    def write_char_array(self, field_name, values):
        raise NotImplementedError()

    def write_int_array(self, field_name, values):
        raise NotImplementedError()

    def write_long_array(self, field_name, values):
        raise NotImplementedError()

    def write_double_array(self, field_name, values):
        raise NotImplementedError()

    def write_float_array(self, field_name, values):
        raise NotImplementedError()

    def write_short_array(self, field_name, values):
        raise NotImplementedError()

    def write_utf_array(self, field_name, values):
        raise NotImplementedError()

    def write_portable_array(self, field_name, values):
        raise NotImplementedError()

    def get_raw_data_output(self):
        raise NotImplementedError()

