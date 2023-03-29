import datetime
import decimal
import sys
import threading
import uuid

import typing

from hazelcast.config import IntType, Config
from hazelcast.errors import HazelcastInstanceNotActiveError, IllegalArgumentError
from hazelcast.serialization.api import IdentifiedDataSerializable, Portable
from hazelcast.serialization.compact import (
    SchemaNotFoundError,
    SchemaNotReplicatedError,
    CompactStreamSerializer,
)
from hazelcast.serialization.data import Data, DATA_OFFSET
from hazelcast.serialization.input import _ObjectDataInput
from hazelcast.serialization.objects import (
    CanonicalizingHashSet,
    IdentifiedAddress,
    IdentifiedMapEntry,
    ReliableTopicMessage,
)
from hazelcast.serialization.output import _ObjectDataOutput
from hazelcast.serialization.portable.classdef import FieldType
from hazelcast.serialization.portable.context import PortableContext
from hazelcast.serialization.portable.serializer import PortableSerializer
from hazelcast.serialization.serializer import *
from hazelcast.util import re_raise

DEFAULT_OUT_BUFFER_SIZE = 4 * 1024


_int_type_to_type_id = {
    IntType.BYTE: CONSTANT_TYPE_BYTE,
    IntType.SHORT: CONSTANT_TYPE_SHORT,
    IntType.INT: CONSTANT_TYPE_INTEGER,
    IntType.LONG: CONSTANT_TYPE_LONG,
    IntType.BIG_INT: JAVA_DEFAULT_TYPE_BIG_INTEGER,
}


def default_partition_strategy(key):
    if hasattr(key, "get_partition_key"):
        return key.get_partition_key()
    return None


def empty_partitioning_strategy(_):
    return None


def handle_exception(e, traceback):
    if isinstance(e, MemoryError):
        re_raise(MemoryError("OUT OF MEMORY"), traceback)
    elif isinstance(
        e, (HazelcastSerializationError, SchemaNotFoundError, SchemaNotReplicatedError)
    ):
        re_raise(e, traceback)
    else:
        re_raise(HazelcastSerializationError(e.args[0]), traceback)


class SerializationServiceV1:
    def __init__(
        self,
        config,
        version=1,
        global_partition_strategy=default_partition_strategy,
        output_buffer_size=DEFAULT_OUT_BUFFER_SIZE,
    ):
        self._version = version
        self._global_partition_strategy = global_partition_strategy
        self._output_buffer_size = output_buffer_size
        self._is_big_endian = config.is_big_endian
        self._active = True
        self._portable_context = PortableContext(self, config.portable_version)
        self.register_class_definitions(
            config.class_definitions, config.check_class_definition_errors
        )
        self._portable_serializer = PortableSerializer(
            self._portable_context, config.portable_factories
        )
        self._compact_stream_serializer = CompactStreamSerializer(config.compact_serializers)

        # merge configured factories with built in ones
        factories = self._get_builtin_identified_factories()
        factories.update(config.data_serializable_factories)
        self._data_serializer = IdentifiedDataSerializer(factories)

        # Register Global Serializer
        self._global_serializer = config.global_serializer() if config.global_serializer else None

        self._null_serializer = NoneSerializer()
        self._python_serializer = PythonObjectSerializer()

        self._registry = SerializerRegistry(
            config,
            self._portable_serializer,
            self._data_serializer,
            self._compact_stream_serializer,
            self._null_serializer,
            self._python_serializer,
            self._global_serializer,
        )

        self._register_constant_serializers()

        # Register Custom Serializers
        for _type, custom_serializer in config.custom_serializers.items():
            self._registry.safe_register_serializer(custom_serializer(), _type)

        # Called here so that we can make sure that we are not overriding
        # any of the default serializers registered above with the Compact
        # serialization.
        self._registry.validate()

    def to_data(self, obj, partitioning_strategy=None):
        """Serialize the input object into byte array representation

        Args:
            obj: Input object
            partitioning_strategy (function): Function in the form of ``lambda key: partitioning_key``.

        Returns:
            hazelcast.serialization.data.Data: Data object
        """
        if obj is None:
            return None

        if isinstance(obj, Data):
            return obj

        out = _ObjectDataOutput(self._output_buffer_size, self, self._is_big_endian)
        try:
            serializer = self._registry.serializer_for(obj)
            partitioning_hash = self._calculate_partitioning_hash(obj, partitioning_strategy)

            out.write_int_big_endian(partitioning_hash)
            out.write_int_big_endian(serializer.get_type_id())
            serializer.write(out, obj)
            return Data(out.to_byte_array())
        except:
            handle_exception(sys.exc_info()[1], sys.exc_info()[2])

    def to_object(self, data):
        """Deserialize input data

        Args:
            data (hazelcast.serialization.data.Data): Serialized input Data object

        Returns:
            any: Deserialized object
        """
        if not isinstance(data, Data):
            return data

        if data.data_size() == 0 and data.get_type() == CONSTANT_TYPE_NULL:
            return None

        inp = _ObjectDataInput(data.buffer, DATA_OFFSET, self, self._is_big_endian)
        try:
            type_id = data.get_type()
            serializer = self._registry.serializer_by_type_id(type_id)
            if serializer is None:
                if self._active:
                    raise HazelcastSerializationError("Missing Serializer for type-id:%s" % type_id)
                else:
                    raise HazelcastInstanceNotActiveError()
            return serializer.read(inp)
        except:
            handle_exception(sys.exc_info()[1], sys.exc_info()[2])

    def write_object(self, out, obj):
        if isinstance(obj, Data):
            raise HazelcastSerializationError(
                "Cannot write a Data instance! Use write_data(out, data) instead."
            )
        try:
            serializer = self._registry.serializer_for(obj)
            out.write_int(serializer.get_type_id())
            serializer.write(out, obj)
        except:
            handle_exception(sys.exc_info()[1], sys.exc_info()[2])

    def read_object(self, inp):
        try:
            type_id = inp.read_int()
            serializer = self._registry.serializer_by_type_id(type_id)
            if serializer is None:
                if self._active:
                    raise HazelcastSerializationError(
                        "Missing Serializer for type-id: %s" % type_id
                    )
                else:
                    raise HazelcastInstanceNotActiveError()
            return serializer.read(inp)
        except:
            handle_exception(sys.exc_info()[1], sys.exc_info()[2])

    def _calculate_partitioning_hash(self, obj, partitioning_strategy):
        partitioning_hash = 0
        _ps = (
            partitioning_strategy
            if partitioning_strategy is not None
            else self._global_partition_strategy
        )
        pk = _ps(obj)
        if pk is not None and pk is not obj:
            partitioning_key = self.to_data(pk, empty_partitioning_strategy)
            partitioning_hash = partitioning_key.get_partition_hash()
        return partitioning_hash

    def destroy(self):
        self._active = False
        self._registry.destroy()

    @property
    def compact_stream_serializer(self) -> CompactStreamSerializer:
        return self._compact_stream_serializer

    def _get_builtin_identified_factories(self):
        return {
            ReliableTopicMessage.FACTORY_ID: {
                ReliableTopicMessage.CLASS_ID: lambda: ReliableTopicMessage(
                    serialization_service=self
                ),
            },
            CanonicalizingHashSet.FACTORY_ID: {
                CanonicalizingHashSet.CLASS_ID: CanonicalizingHashSet,
            },
            IdentifiedMapEntry.FACTORY_ID: {
                IdentifiedMapEntry.CLASS_ID: IdentifiedMapEntry,
            },
            IdentifiedAddress.FACTORY_ID: {
                IdentifiedAddress.CLASS_ID: IdentifiedAddress,
            },
        }

    def _register_constant_serializers(self):
        self._registry.register_constant_serializer(self._null_serializer, type(None))
        self._registry.register_constant_serializer(self._data_serializer)
        self._registry.register_constant_serializer(self._portable_serializer)
        self._registry.register_constant_serializer(self._compact_stream_serializer)
        self._registry.register_constant_serializer(ByteSerializer())
        self._registry.register_constant_serializer(BooleanSerializer(), bool)
        self._registry.register_constant_serializer(CharSerializer())
        self._registry.register_constant_serializer(ShortSerializer())
        self._registry.register_constant_serializer(IntegerSerializer(), int)
        self._registry.register_constant_serializer(LongSerializer())
        self._registry.register_constant_serializer(FloatSerializer())
        self._registry.register_constant_serializer(DoubleSerializer(), float)
        self._registry.register_constant_serializer(UuidSerializer(), uuid.UUID)
        self._registry.register_constant_serializer(StringSerializer(), str)
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
        self._registry.register_constant_serializer(BigIntegerSerializer())
        self._registry.register_constant_serializer(BigDecimalSerializer(), decimal.Decimal)
        self._registry.register_constant_serializer(JavaClassSerializer())
        self._registry.register_constant_serializer(ArraySerializer())
        self._registry.register_constant_serializer(ArrayListSerializer(), list)
        self._registry.register_constant_serializer(LinkedListSerializer())
        self._registry.register_constant_serializer(LocalDateSerializer(), datetime.date)
        self._registry.register_constant_serializer(LocalTimeSerializer(), datetime.time)
        self._registry.register_constant_serializer(LocalDateTimeSerializer())
        self._registry.register_constant_serializer(OffsetDateTimeSerializer(), datetime.datetime)
        self._registry.register_constant_serializer(
            HazelcastJsonValueSerializer(), HazelcastJsonValue
        )

        self._registry.safe_register_serializer(self._python_serializer)

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


class SerializerRegistry:
    def __init__(
        self,
        config: Config,
        portable_serializer: PortableSerializer,
        data_serializer: IdentifiedDataSerializer,
        compact_serializer: CompactStreamSerializer,
        null_serializer: NoneSerializer,
        python_serializer: PythonObjectSerializer,
        global_serializer: typing.Optional[StreamSerializer],
    ):
        self._portable_serializer = portable_serializer
        self._data_serializer = data_serializer
        self._compact_stream_serializer = compact_serializer
        self._null_serializer = null_serializer
        self._python_serializer = python_serializer
        self._global_serializer = global_serializer

        self._constant_type_ids: typing.Dict[int, StreamSerializer] = {}
        self._constant_type_dict: typing.Dict[typing.Type, StreamSerializer] = {}

        self._id_dict: typing.Dict[int, StreamSerializer] = {}
        self._type_dict: typing.Dict[typing.Type, StreamSerializer] = {}

        self._registration_lock = threading.RLock()
        self._int_type_id = _int_type_to_type_id.get(config.default_int_type, None)

        self._compact_types = {c.get_class() for c in config.compact_serializers}

    def serializer_by_type_id(self, type_id):
        """Find and return the serializer for the type-id

        Args:
            type_id (int): Type id of the serializer

        Returns:
          The serializer
        """
        if type_id <= 0:
            index = -type_id
            serializer = self._constant_type_ids.get(index, None)
            if serializer is not None:
                return serializer
        return self._id_dict.get(type_id, None)

    def serializer_for(self, obj):
        """Searches for a serializer for the provided object

        Serializers will be  searched in this order;

        - NULL serializer
        - Default serializers, like primitives, arrays, string and some default types
        - Custom registered types by user
        - Global serializer if registered by user
        - pickle serialization as a fallback

        Args:
            obj: Input object

        Returns:
            The serializer
        """
        # 1-NULL serializer
        if obj is None:
            return self._null_serializer

        obj_type = type(obj)

        # 2-Default serializers, DataSerializable, Portable, Compact,
        # primitives, arrays, String, UUID and some helper types(BigInteger etc)
        serializer = self.lookup_default_serializer(obj_type, obj)

        # 3-Custom registered types by user
        if serializer is None:
            serializer = self.lookup_custom_serializer(obj_type)

        # 5-Global serializer if registered by user
        if serializer is None:
            serializer = self.lookup_global_serializer(obj_type)

        # 4 Internal serializer
        if serializer is None:
            serializer = self.lookup_python_serializer(obj_type)

        if serializer is None:
            raise HazelcastSerializationError(
                "There is no suitable serializer for:" + str(obj_type)
            )
        return serializer

    def lookup_default_serializer(self, obj_type, obj):
        if obj_type in self._compact_types:
            return self._compact_stream_serializer

        if isinstance(obj, IdentifiedDataSerializable):
            return self._data_serializer

        if isinstance(obj, Portable):
            return self._portable_serializer

        if isinstance(obj, str):
            return self.serializer_by_type_id(CONSTANT_TYPE_STRING)

        # LOCATE NUMERIC TYPES
        if obj_type is int:
            type_id = self._int_type_id
            if type_id is None:
                # VAR size
                if MIN_BYTE <= obj <= MAX_BYTE:
                    type_id = CONSTANT_TYPE_BYTE
                elif MIN_SHORT <= obj <= MAX_SHORT:
                    type_id = CONSTANT_TYPE_SHORT
                elif MIN_INT <= obj <= MAX_INT:
                    type_id = CONSTANT_TYPE_INTEGER
                elif MIN_LONG <= obj <= MAX_LONG:
                    type_id = CONSTANT_TYPE_LONG
                else:
                    type_id = JAVA_DEFAULT_TYPE_BIG_INTEGER

            return self.serializer_by_type_id(type_id)

        return self._constant_type_dict.get(obj_type, None)

    def lookup_custom_serializer(self, obj_type):
        serializer = self._type_dict.get(obj_type, None)
        if serializer is not None:
            return serializer

        for super_type in obj_type.__mro__:
            serializer = self.register_from_super_type(obj_type, super_type)
            if serializer is not None:
                return serializer
        return None

    def lookup_python_serializer(self, obj_type):
        self.safe_register_serializer(self._python_serializer, obj_type)
        return self._python_serializer

    def lookup_global_serializer(self, obj_type):
        serializer = self._global_serializer
        if serializer is not None:
            self.safe_register_serializer(serializer, obj_type)
        return serializer

    def register_constant_serializer(self, serializer, object_type=None):
        stream_serializer = serializer
        self._constant_type_ids[-stream_serializer.get_type_id()] = stream_serializer
        if object_type is not None:
            self._constant_type_dict[object_type] = stream_serializer

    def safe_register_serializer(self, stream_serializer, obj_type=None):
        with self._registration_lock:
            if obj_type is not None:
                if obj_type in self._constant_type_dict:
                    raise ValueError("[%s] serializer cannot be overridden!" % obj_type)
                current = self._type_dict.get(obj_type, None)
                if current is not None and current.__class__ != stream_serializer.__class__:
                    raise ValueError(
                        "Serializer[%s] has been already registered for type: %s"
                        % (current.__class__, obj_type)
                    )
                else:
                    self._type_dict[obj_type] = stream_serializer
            serializer_type_id = stream_serializer.get_type_id()
            current = self._id_dict.get(serializer_type_id, None)
            if current is not None and current.__class__ != stream_serializer.__class__:
                raise ValueError(
                    "Serializer[%s] has been already registered for type-id: %s"
                    % (current.__class__, serializer_type_id)
                )
            else:
                self._id_dict[serializer_type_id] = stream_serializer
            return current is None

    def register_from_super_type(self, obj_type, super_type) -> typing.Optional[StreamSerializer]:
        serializer = self._type_dict.get(super_type, None)
        if serializer is not None:
            self.safe_register_serializer(serializer, obj_type)
        return serializer

    def validate(self):
        """
        Makes sure that the classes registered as Compact serializable are not
        overriding the default serializers.

        Must be called in the constructor of the serialization service after it
        completes registering default serializers.
        """
        for compact_type in self._compact_types:
            if compact_type in self._constant_type_dict:
                raise IllegalArgumentError(
                    f"Compact serializer for the class {compact_type}' can not be "
                    f"registered as it overrides the default serializer for that "
                    f"class provided by Hazelcast."
                )

    def destroy(self):
        for serializer in list(self._type_dict.values()):
            serializer.destroy()
        for serializer in list(self._constant_type_dict.values()):
            serializer.destroy()
        self._type_dict.clear()
        self._id_dict.clear()
        self._global_serializer = None
        self._constant_type_dict.clear()
        self._compact_types.clear()
