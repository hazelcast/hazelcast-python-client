import threading

from hazelcast import util
from hazelcast.exception import HazelcastSerializationError
from hazelcast.serialization import bits
from hazelcast.serialization.portable.classdef import ClassDefinition, ClassDefinitionBuilder, FieldType, FieldDefinition
from hazelcast.serialization.portable.writer import ClassDefinitionWriter
from hazelcast.six.moves import range


class PortableContext(object):
    def __init__(self, serialization_service, portable_version):
        self.serialization_service = serialization_service
        self.portable_version = portable_version
        self._class_defs = dict()  # {factory_id:ClassDefinitionContext}

    def get_class_version(self, factory_id, class_id):
        return self._get_class_def_context(factory_id).get_class_version(class_id)

    def set_class_version(self, factory_id, class_id, version):
        self._get_class_def_context(factory_id).set_class_version(class_id, version)

    def lookup_class_definition(self, factory_id, class_id, version):
        return self._get_class_def_context(factory_id).lookup(class_id, version)

    def read_class_definition(self, data_in, factory_id, class_id, version):
        register = True
        builder = ClassDefinitionBuilder(factory_id, class_id, version)

        # final position after portable is read
        data_in.read_int()

        # field count
        field_count = data_in.read_int()
        offset = data_in.position()
        for i in range(0, field_count):
            pos = data_in.read_int(offset + i * bits.INT_SIZE_IN_BYTES)
            data_in.set_position(pos)

            _len = data_in.read_short()
            field_name = bytearray(_len)
            data_in.read_into(field_name)

            field_type = data_in.read_byte()

            field_factory_id = 0
            field_class_id = 0
            field_version = version
            if field_type == FieldType.PORTABLE:
                # is null
                if data_in.read_boolean():
                    register = False
                field_factory_id = data_in.read_int()
                field_class_id = data_in.read_int()

                # TODO: what there's a null inner Portable field
                if register:
                    field_version = data_in.read_int()
                    self.read_class_definition(data_in, field_factory_id, field_class_id, field_version)
            elif field_type == FieldType.PORTABLE_ARRAY:
                k = data_in.read_int()
                field_factory_id = data_in.read_int()
                field_class_id = data_in.read_int()

                # TODO: what there's a null inner Portable field
                if k > 0:
                    p = data_in.read_int()
                    data_in.set_position(p)
                    field_version = data_in.read_int()
                    self.read_class_definition(data_in, field_factory_id, field_class_id, field_version)
                else:
                    register = False
            builder.add_field_def(FieldDefinition(i, field_name.decode('ascii'), field_type, field_version,
                                                  field_factory_id, field_class_id))
        class_def = builder.build()
        if register:
            class_def = self.register_class_definition(class_def)
        return class_def

    def register_class_definition(self, class_definition):
        return self._get_class_def_context(class_definition.factory_id).register(class_definition)

    def lookup_or_register_class_definition(self, portable):
        fid = portable.get_factory_id()
        cid = portable.get_class_id()
        portable_version = util.get_portable_version(portable, self.portable_version)
        class_def = self.lookup_class_definition(fid, cid, portable_version)

        if class_def is None:
            writer = ClassDefinitionWriter(self, fid, cid, portable_version)
            portable.write_portable(writer)
            class_def = writer.register_and_get()

        return class_def

    def _get_class_def_context(self, factory_id):
        if factory_id not in self._class_defs:
            self._class_defs[factory_id] = ClassDefinitionContext(factory_id, self.portable_version)
        return self._class_defs[factory_id]


class ClassDefinitionContext(object):
    def __init__(self, factory_id, portable_version):
        self._factory_id = factory_id
        self._portable_version = portable_version
        self._versioned_definitions = {}  # (class_id, version) : ClassDefinition
        self._current_class_versions = {}  # class_id:version
        self._lock = threading.RLock()

    def get_class_version(self, class_id):
        return self._current_class_versions.get(class_id, -1)

    def set_class_version(self, class_id, version):
        try:
            current_version = self._current_class_versions[class_id]
            if current_version != version:
                raise ValueError("Class-id: {} is already registered!".format(class_id))
        except KeyError:
            self._current_class_versions[class_id] = version

    def lookup(self, class_id, version):
        return self._versioned_definitions.get((class_id, version), None)

    def register(self, class_def):
        with self._lock:
            if class_def is None:
                return None
            if class_def.factory_id != self._factory_id:
                raise HazelcastSerializationError("Invalid factory-id! {} -> {}".format(self._factory_id, class_def))
            if isinstance(class_def, ClassDefinition):
                class_def.set_version_if_not_set(self._portable_version)
            combined_key = (class_def.class_id, class_def.version)
            if combined_key not in self._versioned_definitions:
                self._versioned_definitions[combined_key] = class_def
                return class_def
            current_class_def = self._versioned_definitions[combined_key]
            if isinstance(current_class_def, ClassDefinition):
                if current_class_def != class_def:
                    raise HazelcastSerializationError("Incompatible class-definitions with same class-id: {} vs {}"
                                                      .format(class_def, current_class_def))
                return current_class_def
            self._versioned_definitions[combined_key] = class_def
            return class_def
