from threading import RLock

from api import *
from data import *
from hazelcast.core import *
from serializer import *

EMPTY_PARTITIONING_STRATEGY = lambda key: None


def handle_exception(e):
    if isinstance(e, MemoryError):
        # TODO
        print("OUT OF MEMORY")
        raise e
    elif isinstance(e, HazelcastSerializationError):
        raise e
    else:
        raise HazelcastSerializationError(e)


class HazelcastSerializationError(HazelcastError):
    def __init__(self, message):
        self.message = message


def create_data_output():
    # TODO
    return None


def create_data_input():
    # TODO
    return None


def is_null_data(data):
    return data.data_size() == 0 and data.get_type() == CONSTANT_TYPE_NULL


def index_for_default_type(type_id):
    return -type_id


def is_dataserializable(obj_type):
    return isinstance(obj_type, IdentifiedDataSerializable)


def is_portable(obj_type):
    return isinstance(obj_type, Portable)


def create_buffer_serializer_wrapper(serializer):
    if isinstance(serializer, BufferSerializer):
        return BufferSerializerWrapper(serializer)
    elif isinstance(serializer, StreamSerializer):
        return serializer
    else:
        raise ValueError("Serializer must be instance of either StreamSerializer or ByteArraySerializer!")


class BaseSerializationService(object):
    _global_partition_strategy = None

    def __init__(self, version, global_partition_strategy, output_buffer_size):
        self._registry = SerializerRegistry()
        self._version = version
        self._global_partition_strategy = global_partition_strategy
        self._output_buffer_size = output_buffer_size
        self._active = True

    def to_data(self, obj, partitioning_strategy=_global_partition_strategy):
        """
        Serialize the input object into byte array representation
        :param obj: input object
        :param partitioning_strategy: function in the form of lambda key:partitioning_key
        :return: Data object
        """
        if obj is None:
            return None

        out = create_data_output()
        try:
            serializer = self.serializer_for(obj)
            partitioning_hash = self.__calculate_partitioning_hash(obj, partitioning_strategy)

            out.write_int(partitioning_hash, BIG_ENDIAN)
            out.write_int(serializer.get_type_id(), BIG_ENDIAN)
            serializer.write(out, obj)
            return Data(out.to_buffer())
        except Exception as e:
            handle_exception(e)
        finally:
            pass
            # return out to pool

    def to_object(self, data):
        """
        Deserialize input data
        :param data: serialized input Data object
        :return: Deserialized object
        """
        if not isinstance(data, Data):
            return data
        if is_null_data(data):
            return None

        inp = create_data_input()
        try:
            type_id = data.get_type()
            serializer = self.serializer_by_type_id(type_id)
            if serializer is None:
                if self._active:
                    raise HazelcastSerializationError("Missing Serializer for type-id:{}".format(type_id))
                else:
                    raise HazelcastInstanceNotActiveError()
            return serializer.read(inp)
        except Exception as e:
            handle_exception(e)
        finally:
            pass
            # return out to pool

    def write_object(self, out, obj):
        if isinstance(obj, Data):
            raise HazelcastSerializationError("Cannot write a Data instance! Use write_data(out, data) instead.")
        try:
            serializer = self.serializer_for(obj)
            out.write_int(serializer.get_type_id())
            serializer.write(out, obj)
        except Exception as e:
            handle_exception(e)

    def read_object(self, inp):
        try:
            type_id = inp.read_int()
            serializer = self.serializer_by_type_id(type_id)
            if serializer is None:
                if self._active:
                    raise HazelcastSerializationError("Missing Serializer for type-id:{}".format(type_id))
                else:
                    raise HazelcastInstanceNotActiveError()
            return serializer.read(inp)
        except Exception as e:
            handle_exception(e)

    def __calculate_partitioning_hash(self, obj, partitioning_strategy):
        partitioning_hash = 0
        pk = partitioning_strategy(obj)
        if pk is not None and pk is not obj:
            partitioning_key = self.to_data(pk, EMPTY_PARTITIONING_STRATEGY)
            partitioning_hash = 0 if partitioning_key is None else partitioning_key.get_partition_hash()
        return partitioning_hash


class SerializerRegistry(object):
    def __init__(self):
        self._global_serializer = None
        self._data_serializer = None
        self._null_serializer = None
        self._python_serializer = None

        self._constant_type_ids = []  # array of serializer
        self._constant_type_dict = {}  # dict of class:serializer

        self._id_dic = {}  # dict of type_id:serializer
        self._type_dict = {}  # dict of class:serializer

        self._registration_lock = RLock()

    def serializer_by_type_id(self, type_id):
        """
        Find and return the serializer for the type-id
        :param type_id: type-id the serializer
        :return: the serializer_adaptor
        """
        if type_id <= 0:
            indx = index_for_default_type(type_id)
            if indx < CONSTANT_SERIALIZERS_LENGTH:
                return self._constant_type_ids[indx]
        return self._id_dic[type_id]

    def serializer_for(self, obj):
        """
            Searches for a serializer for the provided object
            Serializers will be  searched in this order;

            1-NULL serializer
            2-Default serializers, like primitives, arrays, string and some default types
            3-Custom registered types by user
            4-marshal serialization if a global serializer with marshal serialization not registered
            5-Global serializer if registered by user

        :param obj: input object
        :return: Serializer
        """
        # 1-NULL serializer
        if obj is None:
            return self._null_serializer_adapter

        obj_type = type(obj)
        serializer = None

        # 2-Default serializers, Dataserializable, Portable, primitives, arrays, String and some helper types(BigInteger etc)
        serializer = self.lookup_default_serializer(obj_type)

        # 3-Custom registered types by user
        if serializer is None:
            serializer = self.lookup_custom_serializer(obj_type)

        # 4 Internal serializer
        if serializer is None and self._global_serializer_adaptor is None:
            serializer = self.lookup_python_serializer(obj_type)

        # 5-Global serializer if registered by user
        if serializer is None:
            serializer = self.lookup_global_serializer(obj_type)

        if serializer is not None:
            if self._active:
                raise HazelcastSerializationError("There is no suitable serializer for:" + str(obj_type))
            else:
                raise HazelcastInstanceNotActiveError()
        return serializer

    def lookup_default_serializer(self, obj_type):
        if is_dataserializable(obj_type):
            raise NotImplementedError()
        if is_portable(obj_type):
            raise NotImplementedError()
        return self._constant_type_dict[obj_type]

    def lookup_custom_serializer(self, obj_type):
        serializer = self._type_dict[obj_type]
        if serializer is not None:
            return serializer
        for super_type in obj_type.__subclasses__():
            serializer = self.register_from_super_type(obj_type, super_type)
            if serializer is not None:
                return serializer
        return None

    def lookup_python_serializer(self, obj_type):
        return self._python_serializer_adapter

    def lookup_global_serializer(self, obj_type):
        return self._global_serializer_adaptor

    def register_constant_serializer(self, obj_type, serializer):
        serializer_adaptor = create_buffer_serializer_wrapper(serializer)
        self.register_constant_serializer_adaptor(obj_type, serializer_adaptor)

    def register_constant_serializer_adaptor(self, obj_type, serializer_adaptor):
        self._constant_type_dict[obj_type] = serializer_adaptor
        self._constant_type_ids[index_for_default_type(serializer_adaptor.get_type_id())] = serializer_adaptor

    def safe_register_serializer(self, obj_type, serializer):
        stream_serializer = create_buffer_serializer_wrapper(serializer)
        with self._registration_lock:
            if self._constant_type_dict.has_key(obj_type):
                raise ValueError("[{}] serializer cannot be overridden!".format(obj_type))
            current = self._type_dict[obj_type]
            if current is not None and current.get_implementation().__class__ != stream_serializer.get_implementation().__class__:
                raise ValueError(
                    "Serializer[{}] has been already registered for type: {}".format(current.get_implementation(), obj_type))
            else:
                self._constant_type_dict[obj_type] = stream_serializer
            current = self._id_dic[stream_serializer.get_type_id()]
            if current is not None and current.get_implementation().__class__ != stream_serializer.get_implementation().__class__:
                raise ValueError(
                    "Serializer[{}] has been already registered for type-id: {}".format(current.get_implementation(),
                                                                                        stream_serializer.get_type_id()))
            else:
                self._id_dic[stream_serializer.get_type_id()] = stream_serializer
            return current is None

    def register_from_super_type(self, obj_type, super_type):
        serializer_adaptor = self._type_dict[super_type]
        if serializer_adaptor is not None:
            self.safe_register_serializer_adaptor(obj_type, serializer_adaptor)
        return serializer_adaptor
