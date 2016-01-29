from hazelcast.protocol.codec import atomic_long_add_and_get_codec, atomic_long_compare_and_set_codec, \
    atomic_long_decrement_and_get_codec, atomic_long_get_and_add_codec, atomic_long_get_and_increment_codec, \
    atomic_long_get_and_set_codec, atomic_long_get_codec, atomic_long_increment_and_get_codec, atomic_long_set_codec, \
    atomic_long_alter_and_get_codec, atomic_long_alter_codec, atomic_long_apply_codec, atomic_long_get_and_alter_codec
from hazelcast.proxy.base import PartitionSpecificProxy
from hazelcast.util import check_not_none


class AtomicLong(PartitionSpecificProxy):
    def add_and_get(self, delta):
        return self._encode_invoke(atomic_long_add_and_get_codec, delta=delta)

    def alter(self, function):
        check_not_none(function, "function can't be None")
        return self._encode_invoke(atomic_long_alter_codec, function=self._to_data(function))

    def alter_and_get(self, function):
        check_not_none(function, "function can't be None")
        return self._encode_invoke(atomic_long_alter_and_get_codec, function=self._to_data(function))

    def apply(self, function):
        check_not_none(function, "function can't be None")
        return self._encode_invoke(atomic_long_apply_codec, function=self._to_data(function))

    def compare_and_set(self, expected, updated):
        return self._encode_invoke(atomic_long_compare_and_set_codec, expected=expected,
                                   updated=updated)

    def decrement_and_get(self):
        return self._encode_invoke(atomic_long_decrement_and_get_codec)

    def get(self):
        return self._encode_invoke(atomic_long_get_codec)

    def get_and_add(self, delta):
        return self._encode_invoke(atomic_long_get_and_add_codec, delta=delta)

    def get_and_alter(self, function):
        check_not_none(function, "function can't be None")
        return self._encode_invoke(atomic_long_get_and_alter_codec, function=self._to_data(function))

    def get_and_set(self, new_value):
        return self._encode_invoke(atomic_long_get_and_set_codec, new_value=new_value)

    def increment_and_get(self):
        return self._encode_invoke(atomic_long_increment_and_get_codec)

    def get_and_increment(self):
        return self._encode_invoke(atomic_long_get_and_increment_codec)

    def set(self, new_value):
        return self._encode_invoke(atomic_long_set_codec, new_value=new_value)
