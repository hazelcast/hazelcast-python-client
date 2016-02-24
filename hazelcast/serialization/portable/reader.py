from hazelcast.serialization.api import PortableReader


class DefaultPortableReader(PortableReader):
    def read_utf_array(self, field_name):
        pass

    def read_utf(self, field_name):
        pass

    def read_short_array(self, field_name):
        pass

    def read_short(self, field_name):
        pass

    def read_portable_array(self, field_name):
        pass

    def read_portable(self, field_name):
        pass

    def read_long_array(self, field_name):
        pass

    def read_long(self, field_name):
        pass

    def read_int_array(self, field_name):
        pass

    def read_int(self, field_name):
        pass

    def read_float_array(self, field_name):
        pass

    def read_float(self, field_name):
        pass

    def read_double_array(self, field_name):
        pass

    def read_double(self, field_name):
        pass

    def read_char_array(self, field_name):
        pass

    def read_char(self, field_name):
        pass

    def read_byte_array(self, field_name):
        pass

    def read_byte(self, field_name):
        pass

    def read_boolean_array(self, field_name):
        pass

    def read_boolean(self, field_name):
        pass

    def has_field(self, field_name):
        pass

    def get_version(self):
        pass

    def get_raw_data_input(self):
        pass

    def get_field_type(self, field_name):
        pass

    def get_field_names(self):
        pass

    def get_field_class_id(self, field_name):
        pass


class MorphingPortableReader(DefaultPortableReader):
    pass
