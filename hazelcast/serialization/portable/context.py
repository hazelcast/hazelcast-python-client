class PortableContext(object):
    # int getVersion();
    def get_version(self):
        pass

    # int getClassVersion(int factoryId, int classId);
    def get_class_version(self):
        pass

    # void setClassVersion(int factoryId, int classId, int version);
    def set_class_version(self):
        pass

    # ClassDefinition lookupClassDefinition(int factoryId, int classId, int version);
    def lookup_class_definition(self, factory_id, class_id, version):
        pass

    # ClassDefinition registerClassDefinition(ClassDefinition cd);
    def register_class_definition(self, class_definition):
        pass

    # ClassDefinition lookupOrRegisterClassDefinition(Portable portable) throws IOException;
    def lookup_or_register_class_definition(self, portable):
        pass

    # FieldDefinition getFieldDefinition(ClassDefinition cd, String name);
    def get_field_definition(self, class_definition, name):
        pass

    # ByteOrder getByteOrder();
    def get_byte_order(self):
        pass

