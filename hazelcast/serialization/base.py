import sys
from threading import RLock

from hazelcast.config import IntType
from hazelcast.serialization.api import *
from hazelcast.serialization.data import *
from hazelcast.errors import HazelcastInstanceNotActiveError, HazelcastSerializationError
from hazelcast.serialization.input import _ObjectDataInput
from hazelcast.serialization.output import _ObjectDataOutput
from hazelcast.serialization.serializer import *
from hazelcast import six


_int_type_to_type_id = {
    IntType.BYTE: CONSTANT_TYPE_BYTE,
    IntType.SHORT: CONSTANT_TYPE_SHORT,
    IntType.INT: CONSTANT_TYPE_INTEGER,
    IntType.LONG: CONSTANT_TYPE_LONG,
    IntType.BIG_INT: JAVA_DEFAULT_TYPE_BIG_INTEGER,
}


def empty_partitioning_strategy(_):
    return None


def handle_exception(e, traceback):
    if isinstance(e, MemoryError):
        # TODO
        six.print_("OUT OF MEMORY")
        six.reraise(MemoryError, e, traceback)
    elif isinstance(e, HazelcastSerializationError):
        six.reraise(HazelcastSerializationError, e, traceback)
    else:
        six.reraise(HazelcastSerializationError, HazelcastSerializationError(e.args[0]), traceback)


def is_null_data(data):
    return data.data_size() == 0 and data.get_type() == CONSTANT_TYPE_NULL


def index_for_default_type(type_id):
    return -type_id


class BaseSerializationService(object):
    def __init__(self, version, global_partition_strategy, output_buffer_size, is_big_endian, int_type):
        self._registry = SerializerRegistry(int_type)
        self._version = version
        self._global_partition_strategy = global_partition_strategy
        self._output_buffer_size = output_buffer_size
        self._is_big_endian = is_big_endian
        self._active = True

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

        out = self._create_data_output()
        try:
            serializer = self._registry.serializer_for(obj)
            partitioning_hash = self._calculate_partitioning_hash(obj, partitioning_strategy)

            out.write_int_big_endian(partitioning_hash)
            out.write_int_big_endian(serializer.get_type_id())
            serializer.write(out, obj)
            return Data(out.to_byte_array())
        except:
            handle_exception(sys.exc_info()[1], sys.exc_info()[2])
        finally:
            pass
            # return out to pool

    def to_object(self, data):
        """Deserialize input data

        Args:
            data (hazelcast.serialization.data.Data): Serialized input Data object

        Returns:
            any: Deserialized object
        """
        if not isinstance(data, Data):
            return data

        if is_null_data(data):
            return None

        inp = self._create_data_input(data)
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
        finally:
            pass
            # return out to pool

    def write_object(self, out, obj):
        if isinstance(obj, Data):
            raise HazelcastSerializationError("Cannot write a Data instance! Use write_data(out, data) instead.")
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
                    raise HazelcastSerializationError("Missing Serializer for type-id: %s" % type_id)
                else:
                    raise HazelcastInstanceNotActiveError()
            return serializer.read(inp)
        except:
            handle_exception(sys.exc_info()[1], sys.exc_info()[2])

    def _calculate_partitioning_hash(self, obj, partitioning_strategy):
        partitioning_hash = 0
        _ps = partitioning_strategy if partitioning_strategy is not None else self._global_partition_strategy
        pk = _ps(obj)
        if pk is not None and pk is not obj:
            partitioning_key = self.to_data(pk, empty_partitioning_strategy)
            partitioning_hash = 0 if partitioning_key is None else partitioning_key.get_partition_hash()
        return partitioning_hash

    def _create_data_output(self):
        return _ObjectDataOutput(self._output_buffer_size, self, self._is_big_endian)

    def _create_data_input(self, data):
        return _ObjectDataInput(data._buffer, DATA_OFFSET, self, self._is_big_endian)

    def destroy(self):
        self._active = False
        self._registry.destroy()


class SerializerRegistry(object):
    def __init__(self, int_type):
        self._global_serializer = None
        self._portable_serializer = None
        self._data_serializer = None
        self._null_serializer = NoneSerializer()
        self._python_serializer = PythonObjectSerializer()

        self._constant_type_ids = {}  # dict of id:serializer
        self._constant_type_dict = {}  # dict of class:serializer

        self._id_dic = {}  # dict of type_id:serializer
        self._type_dict = {}  # dict of class:serializer

        self._registration_lock = RLock()
        self.int_type = int_type
        self._int_type_id = _int_type_to_type_id.get(int_type, None)

    def serializer_by_type_id(self, type_id):
        """Find and return the serializer for the type-id

        Args:
            type_id (int): Type id of the serializer

        Returns:
          The serializer
        """
        if type_id <= 0:
            indx = index_for_default_type(type_id)
            serializer = self._constant_type_ids.get(indx, None)
            if serializer is not None:
                return serializer
        return self._id_dic.get(type_id, None)

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

        # 2-Default serializers, DataSerializable, Portable, primitives, arrays, String, UUID
        # and some helper types(BigInteger etc)
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
            raise HazelcastSerializationError("There is no suitable serializer for:" + str(obj_type))
        return serializer

    def lookup_default_serializer(self, obj_type, obj):
        if isinstance(obj, IdentifiedDataSerializable):
            return self._data_serializer

        if isinstance(obj, Portable):
            return self._portable_serializer

        if isinstance(obj, six.string_types):
            return self.serializer_by_type_id(CONSTANT_TYPE_STRING)

        # LOCATE NUMERIC TYPES
        if obj_type in six.integer_types:
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
        for super_type in obj_type.__subclasses__():
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
        self._constant_type_ids[index_for_default_type(stream_serializer.get_type_id())] = stream_serializer
        if object_type is not None:
            self._constant_type_dict[object_type] = stream_serializer

    def safe_register_serializer(self, stream_serializer, obj_type=None):
        with self._registration_lock:
            if obj_type is not None:
                if obj_type in self._constant_type_dict:
                    raise ValueError("[%s] serializer cannot be overridden!" % obj_type)
                current = self._type_dict.get(obj_type, None)
                if current is not None and current.__class__ != stream_serializer.__class__:
                    raise ValueError("Serializer[%s] has been already registered for type: %s"
                                     % (current.__class__, obj_type))
                else:
                    self._type_dict[obj_type] = stream_serializer
            serializer_type_id = stream_serializer.get_type_id()
            current = self._id_dic.get(serializer_type_id, None)
            if current is not None and current.__class__ != stream_serializer.__class__:
                raise ValueError("Serializer[%s] has been already registered for type-id: %s"
                                 % (current.__class__, serializer_type_id))
            else:
                self._id_dic[serializer_type_id] = stream_serializer
            return current is None

    def register_from_super_type(self, obj_type, super_type):
        serializer = self._type_dict.get(super_type, None)
        if serializer is not None:
            self.safe_register_serializer(serializer, obj_type)
        return serializer

    def destroy(self):
        for serializer in list(self._type_dict.values()):
            serializer.destroy()
        for serializer in list(self._constant_type_dict.values()):
            serializer.destroy()
        self._type_dict.clear()
        self._id_dic.clear()
        self._global_serializer = None
        self._constant_type_dict.clear()
