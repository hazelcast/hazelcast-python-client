import uuid

from hazelcast.serialization.base import BaseSerializationService
from hazelcast.serialization.portable.classdef import FieldType
from hazelcast.serialization.portable.context import PortableContext
from hazelcast.serialization.portable.serializer import PortableSerializer
from hazelcast.serialization.serializer import *
from hazelcast import six

DEFAULT_OUT_BUFFER_SIZE = 4 * 1024


def default_partition_strategy(key):
    if hasattr(key, "get_partition_key"):
        return key.get_partition_key()
    return None


class SerializationServiceV1(BaseSerializationService):
    def __init__(
        self,
        config,
        version=1,
        global_partition_strategy=default_partition_strategy,
        output_buffer_size=DEFAULT_OUT_BUFFER_SIZE,
    ):
        super(SerializationServiceV1, self).__init__(
            version,
            global_partition_strategy,
            output_buffer_size,
            config.is_big_endian,
            config.default_int_type,
        )
        self._portable_context = PortableContext(self, config.portable_version)
        self.register_class_definitions(
            config.class_definitions, config.check_class_definition_errors
        )
        self._registry._portable_serializer = PortableSerializer(
            self._portable_context, config.portable_factories
        )

        # merge configured factories with built in ones
        factories = {}
        factories.update(config.data_serializable_factories)
        self._registry._data_serializer = IdentifiedDataSerializer(factories)
        self._register_constant_serializers()

        # Register Custom Serializers
        for _type, custom_serializer in six.iteritems(config.custom_serializers):
            self._registry.safe_register_serializer(custom_serializer(), _type)

        # Register Global Serializer
        global_serializer = config.global_serializer
        if global_serializer:
            self._registry._global_serializer = global_serializer()

    def _register_constant_serializers(self):
        self._registry.register_constant_serializer(self._registry._null_serializer, type(None))
        self._registry.register_constant_serializer(self._registry._data_serializer)
        self._registry.register_constant_serializer(self._registry._portable_serializer)
        self._registry.register_constant_serializer(ByteSerializer())
        self._registry.register_constant_serializer(BooleanSerializer(), bool)
        self._registry.register_constant_serializer(CharSerializer())
        self._registry.register_constant_serializer(ShortSerializer())
        self._registry.register_constant_serializer(IntegerSerializer())
        self._registry.register_constant_serializer(LongSerializer())
        self._registry.register_constant_serializer(FloatSerializer())
        self._registry.register_constant_serializer(DoubleSerializer(), float)
        self._registry.register_constant_serializer(UuidSerializer(), uuid.UUID)
        self._registry.register_constant_serializer(StringSerializer())
        # Arrays of primitives and String
        self._registry.register_constant_serializer(ByteArraySerializer(), bytearray)
        self._registry.register_constant_serializer(BooleanArraySerializer())
        self._registry.register_constant_serializer(CharArraySerializer())
        self._registry.register_constant_serializer(ShortArraySerializer())
        self._registry.register_constant_serializer(IntegerArraySerializer())
        self._registry.register_constant_serializer(LongArraySerializer())
        self._registry.register_constant_serializer(FloatArraySerializer())
        self._registry.register_constant_serializer(DoubleArraySerializer())
        self._registry.register_constant_serializer(StringArraySerializer())
        # EXTENSIONS
        self._registry.register_constant_serializer(DateTimeSerializer(), datetime)
        self._registry.register_constant_serializer(BigIntegerSerializer())
        self._registry.register_constant_serializer(JavaClassSerializer())
        self._registry.register_constant_serializer(ArrayListSerializer(), list)
        self._registry.register_constant_serializer(LinkedListSerializer())
        self._registry.register_constant_serializer(
            HazelcastJsonValueSerializer(), HazelcastJsonValue
        )

        self._registry.safe_register_serializer(self._registry._python_serializer)

    def register_class_definitions(self, class_definitions, check_error):
        factories = dict()
        for cd in class_definitions:
            factory_id = cd.factory_id
            class_defs = factories.get(factory_id, None)
            if class_defs is None:
                class_defs = dict()
                factories[factory_id] = class_defs

            class_id = cd.class_id
            if class_id in class_defs:
                raise HazelcastSerializationError(
                    "Duplicate registration found for class-id: %s" % class_id
                )
            class_defs[class_id] = cd

        for cd in class_definitions:
            self.register_class_definition(cd, factories, check_error)

    def register_class_definition(self, class_definition, factories, check_error):
        field_names = class_definition.get_field_names()
        for field_name in field_names:
            fd = class_definition.get_field(field_name)
            if fd.field_type == FieldType.PORTABLE or fd.field_type == FieldType.PORTABLE_ARRAY:
                factory_id = fd.factory_id
                class_id = fd.class_id
                class_defs = factories.get(factory_id, None)
                if class_defs is not None:
                    nested_cd = class_defs.get(class_id, None)
                    if nested_cd is not None:
                        self.register_class_definition(nested_cd, factories, check_error)
                        self._portable_context.register_class_definition(nested_cd)
                        continue

                if check_error:
                    raise HazelcastSerializationError(
                        "Could not find registered ClassDefinition for factory-id: %s, class-id: %s"
                        % (factory_id, class_id)
                    )

        self._portable_context.register_class_definition(class_definition)
