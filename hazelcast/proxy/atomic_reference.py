from hazelcast.protocol.codec import atomic_reference_compare_and_set_codec, atomic_reference_clear_codec, \
    atomic_reference_contains_codec, atomic_reference_get_and_set_codec, atomic_reference_set_and_get_codec, \
    atomic_reference_get_codec, atomic_reference_is_null_codec, atomic_reference_set_codec, \
    atomic_reference_alter_and_get_codec, atomic_reference_alter_codec, atomic_reference_apply_codec, \
    atomic_reference_get_and_alter_codec
from hazelcast.proxy.base import PartitionSpecificProxy
from hazelcast.util import check_not_none


class AtomicReference(PartitionSpecificProxy):
    def alter(self, function):
        check_not_none(function, "function can't be None")
        return self._encode_invoke(atomic_reference_alter_codec, function=self._to_data(function))

    def apply(self, function):
        check_not_none(function, "function can't be None")
        return self._encode_invoke(atomic_reference_apply_codec, function=self._to_data(function))

    def alter_and_get(self, function):
        check_not_none(function, "function can't be None")
        return self._encode_invoke(atomic_reference_alter_and_get_codec, function=self._to_data(function))

    def compare_and_set(self, expected, updated):
        return self._encode_invoke(atomic_reference_compare_and_set_codec,
                                   expected=self._to_data(expected), updated=self._to_data(updated))

    def clear(self):
        return self._encode_invoke(atomic_reference_clear_codec)

    def contains(self, expected):
        return self._encode_invoke(atomic_reference_contains_codec,
                                   expected=self._to_data(expected))

    def get(self):
        return self._encode_invoke(atomic_reference_get_codec)

    def get_and_alter(self, function):
        check_not_none(function, "function can't be None")
        return self._encode_invoke(atomic_reference_get_and_alter_codec, function=self._to_data(function))

    def get_and_set(self, new_value):
        return self._encode_invoke(atomic_reference_get_and_set_codec,
                                   new_value=self._to_data(new_value))

    def is_null(self):
        return self._encode_invoke(atomic_reference_is_null_codec)

    def set(self, new_value):
        return self._encode_invoke(atomic_reference_set_codec,
                                   new_value=self._to_data(new_value))

    def set_and_get(self, new_value):
        return self._encode_invoke(atomic_reference_set_and_get_codec,
                                   new_value=self._to_data(new_value))
