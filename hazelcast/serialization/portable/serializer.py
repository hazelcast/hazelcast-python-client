import hazelcast.util as util
from hazelcast.exception import HazelcastSerializationError
from hazelcast.serialization.api import StreamSerializer, Portable
from hazelcast.serialization.portable.reader import DefaultPortableReader, MorphingPortableReader
from hazelcast.serialization.portable.writer import DefaultPortableWriter
from hazelcast.serialization.serialization_const import CONSTANT_TYPE_PORTABLE


class PortableSerializer(StreamSerializer):
    def __init__(self, portable_context, portable_factories):
        self._portable_context = portable_context
        self._portable_factories = portable_factories

    def write(self, out, portable):
        if not isinstance(portable, Portable):
            raise ValueError("Input object must be an instance of Portable")
        if portable.get_class_id() == 0:
            raise ValueError("Portable class id cannot be zero!")
        out.write_int(portable.get_factory_id())
        out.write_int(portable.get_class_id())
        self.write_internal(out, portable)

    def write_internal(self, out, portable):
        cd = self._portable_context.lookup_or_register_class_definition(portable)
        out.write_int(cd.version)

        writer = DefaultPortableWriter(self, out, cd)
        portable.write_portable(writer)
        writer.end()

    def read(self, inp):
        factory_id = inp.read_int()
        class_id = inp.read_int()
        return self.read_internal(inp, factory_id, class_id)

    def read_internal(self, inp, factory_id, class_id):
        version = inp.read_int()
        portable = self.create_new_portable_instance(factory_id, class_id)
        portable_version = self.find_portable_version(factory_id, class_id, portable)
        reader = self.create_reader(inp, factory_id, class_id, version, portable_version)
        portable.read_portable(reader)
        reader.end()
        return portable

    def find_portable_version(self, factory_id, class_id, portable):
        current_version = self._portable_context.get_class_version(factory_id, class_id)
        if current_version < 0:
            current_version = util.get_portable_version(portable, self._portable_context.portable_version)
        if current_version > 0:
            self._portable_context.set_class_version(factory_id, class_id, current_version)
        return current_version

    def create_new_portable_instance(self, factory_id, class_id):
        try:
            portable_factory = self._portable_factories[factory_id]
        except KeyError:
            raise HazelcastSerializationError("Could not find portable_factory for factory-id: {}".format(factory_id))

        portable = portable_factory[class_id]
        if portable is None:
            raise HazelcastSerializationError("Could not create Portable for class-id: {}".format(class_id))
        return portable()

    def create_reader(self, inp, factory_id, class_id, version, portable_version):
        effective_version = version
        if version < 0:
            effective_version = self._portable_context.getVersion()

        cd = self._portable_context.lookup_class_definition(factory_id, class_id, effective_version)
        if cd is None:
            begin = inp.position()
            cd = self._portable_context.read_class_definition(inp, factory_id, class_id, effective_version)
            inp.set_position(begin)

        if portable_version == effective_version:
            reader = DefaultPortableReader(self, inp, cd)
        else:
            reader = MorphingPortableReader(self, inp, cd)
        return reader

    def create_morphing_reader(self, inp):
        factory_id = inp.read_int()
        class_id = inp.read_int()
        version = inp.read_int()

        portable = self.create_new_portable_instance(factory_id, class_id)
        portable_version = self.find_portable_version(factory_id, class_id, portable)

        return self.create_reader(inp, factory_id, class_id, version, portable_version)

    def create_default_reader(self, inp):
        factory_id = inp.read_int()
        class_id = inp.read_int()
        version = inp.read_int()

        return self.create_reader(inp, factory_id, class_id, version, version)

    def get_type_id(self):
        return CONSTANT_TYPE_PORTABLE

    def destroy(self):
        self._portable_factories.clear()
