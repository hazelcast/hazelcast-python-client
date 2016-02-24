from hazelcast.serialization.api import PortableWriter


class DefaultPortableWriter(PortableWriter):
    def __init__(self, portable_serializer, out, class_definition):
        self._portable_serializer = portable_serializer
        self._out = out
        self._class_definition = class_definition
        self._writen_fields = set()
        self._begin_pos = out.position()

        # room for final offset
        out.write_zero_bytes(4)

    def write_utf_array(self, field_name, values):
        pass

    def write_utf(self, field_name, value):
        pass

    def write_short_array(self, field_name, values):
        pass

    def write_short(self, field_name, value):
        pass

    def write_portable_array(self, field_name, values):
        pass

    def write_portable(self, field_name, portable):
        pass

    def write_null_portable(self, field_name, factory_id, class_id):
        pass

    def write_long_array(self, field_name, values):
        pass

    def write_long(self, field_name, value):
        pass

    def write_int_array(self, field_name, values):
        pass

    def write_int(self, field_name, value):
        pass

    def write_float_array(self, field_name, values):
        pass

    def write_float(self, field_name, value):
        pass

    def write_double_array(self, field_name, values):
        pass

    def write_double(self, field_name, value):
        pass

    def write_char_array(self, field_name, values):
        pass

    def write_char(self, field_name, value):
        pass

    def write_byte_array(self, field_name, values):
        pass

    def write_byte(self, field_name, value):
        pass

    def write_boolean_array(self, field_name, values):
        pass

    def write_boolean(self, field_name, value):
        pass

    def get_raw_data_output(self):
        pass
