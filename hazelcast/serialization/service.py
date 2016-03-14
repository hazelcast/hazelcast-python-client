import logging

from hazelcast.serialization.base import BaseSerializationService
from hazelcast.serialization.portable.context import PortableContext
from hazelcast.serialization.portable.serializer import PortableSerializer
from hazelcast.serialization.serializer import *

DEFAULT_OUT_BUFFER_SIZE = 4 * 1024


def default_partition_strategy(key):
    if hasattr(key, "get_partition_key"):
        return key.get_partition_key()
    return None


class SerializationServiceV1(BaseSerializationService):
    logger = logging.getLogger("SerializationService")

    def __init__(self, serialization_config=None, version=1, portable_version=0,
                 global_partition_strategy=default_partition_strategy,
                 output_buffer_size=DEFAULT_OUT_BUFFER_SIZE, is_big_endian=True):
        super(SerializationServiceV1, self).__init__(version, global_partition_strategy, output_buffer_size, is_big_endian)
        self.serialization_config = serialization_config

        self._portable_context = PortableContext(self, portable_version)
        for class_def in serialization_config.class_definitions:
            self._portable_context.register_class_definition(class_def)
        self._registry._portable_serializer = PortableSerializer(self._portable_context, self.serialization_config.portable_factories)

        # merge configured factories with built in ones
        factories = {}
        factories.update(self.serialization_config.data_serializable_factories)
        self._registry._data_serializer = IdentifiedDataSerializer(factories)
        self._register_constant_serializers()

        # Register Custom Serializers
        for _type, custom_serializer in serialization_config.custom_serializers.iteritems():
            self._registry.safe_register_serializer(custom_serializer(), _type)

        # Register Global Serializer
        global_serializer = serialization_config.global_serializer
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
        self._registry.register_constant_serializer(DoubleSerializer())
        self._registry.register_constant_serializer(StringSerializer())
        # Arrays of primitives and String
        self._registry.register_constant_serializer(ByteArraySerializer())
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
        self._registry.register_constant_serializer(BigDecimalSerializer())
        self._registry.register_constant_serializer(JavaClassSerializer())
        self._registry.register_constant_serializer(JavaEnumSerializer())
        self._registry.register_constant_serializer(ArrayListSerializer(), list)
        self._registry.register_constant_serializer(LinkedListSerializer())

        self._registry.safe_register_serializer(self._registry._python_serializer)
