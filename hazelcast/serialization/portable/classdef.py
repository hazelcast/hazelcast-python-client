class FieldDefinition(object):
    def __init__(self, index, field_name, field_type, factory_id, class_id):
        self.index = index
        self.field_name = field_name
        self.field_type = field_type
        self.factory_id = factory_id
        self.class_id = class_id


class ClassDefinition(object):
    def __init__(self, factory_id, class_id, version):
        self.factory_id = factory_id
        self.class_id = class_id
        self.version = version
        self.fieldDefinitions = {}  # string:FieldDefinition

    def get_field(self, field_name_or_index):
        if isinstance(field_name_or_index, int):
            index = field_name_or_index
            count = self.get_field_count()
            if 0 <= index < count:
                for field in self.fieldDefinitions.itervalues():
                    if field.index == index:
                        return field
            raise IndexError("Index is out of bound. Index: {} and size: {}".format(index, count))
        else:
            return self.fieldDefinitions.get(field_name_or_index, None)

    def has_field(self, field_name):
        return self.fieldDefinitions.has_key(field_name)

    def get_field_names(self):
        return self.fieldDefinitions.keys()

    def get_field_type(self, field_name):
        fd = self.get_field(field_name)
        if fd:
            return fd.field_type
        raise ValueError("Unknown field: {}".format(field_name))

    def get_field_class_id(self, field_name):
        fd = self.get_field(field_name)
        if fd:
            return fd.class_id
        raise ValueError("Unknown field: {}".format(field_name))

    def get_field_count(self):
        return len(self.fieldDefinitions)
