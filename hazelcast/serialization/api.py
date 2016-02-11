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
        pass

    def write_boolean(self, val):
        """
        Writes a boolean value to this output stream
        single byte value 1 represent True, 0 represent False
        :param val: the boolean to be written
        """
        pass

    def write_byte(self, val):
        pass

    def write_short(self, val):
        pass

    def write_char(self, val):
        pass

    def write_int(self, val):
        pass

    def write_long(self, val):
        pass

    def write_float(self, val):
        pass

    def write_double(self, val):
        pass

    def write_bytes(self, string):
        pass

    def write_chars(self, val):
        pass

    def write_utf(self, val):
        pass

    def write_byte_array(self, val):
        pass

    def write_boolean_array(self, val):
        pass

    def write_char_array(self, val):
        pass

    def write_int_array(self, val):
        pass

    def write_long_array(self, val):
        pass

    def write_double_array(self, val):
        pass

    def write_float_array(self, val):
        pass

    def write_short_array(self, val):
        pass

    def write_utf_array(self, val):
        pass

    def write_object(self, val):
        pass

    def write_data(self, val):
        pass

    def to_byte_array(self):
        pass

    def get_byte_order(self):
        pass


class ObjectDataInput(object):
    def read_into(self, buff, offset=None, length=None):
        pass

    def skip_bytes(self, count):
        pass

    def read_boolean(self):
        pass

    def read_byte(self):
        pass

    def read_unsigned_byte(self):
        pass

    def read_short(self):
        pass

    def read_unsigned_short(self):
        pass

    def read_int(self):
        pass

    def read_long(self):
        pass

    def read_float(self):
        pass

    def read_double(self):
        pass

    def read_utf(self):
        pass

    def read_byte_array(self):
        pass

    def read_boolean_array(self):
        pass

    def read_char_array(self):
        pass

    def read_int_array(self):
        pass

    def read_long_array(self):
        pass

    def read_double_array(self):
        pass

    def read_float_array(self):
        pass

    def read_short_array(self):
        pass

    def read_utf_array(self):
        pass

    def read_object(self):
        pass

    def read_data(self):
        pass

    def get_byte_order(self):
        pass


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
    def write_portable(self):
        pass

    def read_portable(self):
        pass

    def get_factory_id(self):
        pass

    def get_class_id(self):
        pass


class StreamSerializer(object):
    def write(self, out, obj):
        raise NotImplementedError("write method must be implemented")

    def read(self, inp):
        raise NotImplementedError("write method must be implemented")

    def get_type_id(self):
        raise NotImplementedError("get_type_id must be implemented")

    def destroy(self):
        pass
