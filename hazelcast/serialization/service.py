import logging

from hazelcast.serialization import predicate
from hazelcast.serialization.base import BaseSerializationService
from hazelcast.serialization.serializer import *

DEFAULT_OUT_BUFFER_SIZE = 4 * 1024


def default_partition_strategy(key):
    if hasattr(key, "get_partition_key"):
        return key.get_partition_key()
    return None


class SerializationServiceV1(BaseSerializationService):
    logger = logging.getLogger("SerializationService")

    def __init__(self, serialization_config=None, version=1, global_partition_strategy=default_partition_strategy,
                 output_buffer_size=DEFAULT_OUT_BUFFER_SIZE,
                 is_big_endian=True):
        super(SerializationServiceV1, self).__init__(version, global_partition_strategy, output_buffer_size,
                                                     is_big_endian)
        self.serialization_config = serialization_config

        # merge configured factories with built in ones
        factories = {}
        factories.update(predicate.FACTORY)
        factories.update(self.serialization_config.data_serializable_factories)

        self._registry._data_serializer = IdentifiedDataSerializer(
            factories)
        self._register_constant_serializers()

    def _register_constant_serializers(self):
        self._registry.register_constant_serializer(self._registry._null_serializer, type(None))
        self._registry.register_constant_serializer(self._registry._data_serializer)
        # self._registry.register_constant_serializer(self._registry._portable_serializer)
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
