from hazelcast.protocol.codec import atomic_reference_compare_and_set_codec, atomic_reference_clear_codec, \
    atomic_reference_contains_codec, atomic_reference_get_and_set_codec, atomic_reference_set_and_get_codec, \
    atomic_reference_get_codec, atomic_reference_is_null_codec, atomic_reference_set_codec
from hazelcast.proxy.base import PartitionSpecificProxy


class AtomicReference(PartitionSpecificProxy):
    def compare_and_set(self, expected, updated):
        return self._encode_invoke_on_partition(atomic_reference_compare_and_set_codec, name=self.name,
                                                expected=self._to_data(expected), updated=self._to_data(updated))

    def get(self):
        return self._encode_invoke_on_partition(atomic_reference_get_codec, name=self.name)

    def set(self, new_value):
        return self._encode_invoke_on_partition(atomic_reference_set_codec, name=self.name,
                                                new_value=self._to_data(new_value))

    def get_and_set(self, new_value):
        return self._encode_invoke_on_partition(atomic_reference_get_and_set_codec, name=self.name,
                                                new_value=self._to_data(new_value))

    def set_and_get(self, new_value):
        return self._encode_invoke_on_partition(atomic_reference_set_and_get_codec, name=self.name,
                                                new_value=self._to_data(new_value))

    def is_null(self):
        return self._encode_invoke_on_partition(atomic_reference_is_null_codec, name=self.name)

    def clear(self):
        return self._encode_invoke_on_partition(atomic_reference_clear_codec, name=self.name)

    def contains(self, expected):
        return self._encode_invoke_on_partition(atomic_reference_contains_codec, name=self.name,
                                                expected=self._to_data(expected))

    def __str__(self):
        return "AtomicReference(name=%s)" % self.name
