from hazelcast.protocol.codec import atomic_reference_compare_and_set_codec, atomic_reference_clear_codec, \
    atomic_reference_contains_codec, atomic_reference_get_and_set_codec, atomic_reference_set_and_get_codec, \
    atomic_reference_get_codec, atomic_reference_is_null_codec, atomic_reference_set_codec, \
    atomic_reference_alter_and_get_codec, atomic_reference_alter_codec, atomic_reference_apply_codec, \
    atomic_reference_get_and_alter_codec
from hazelcast.proxy.base import PartitionSpecificProxy
from hazelcast.util import check_not_none


class AtomicReference(PartitionSpecificProxy):
    """
    AtomicReference is a atomically updated reference to an object.
    """
    def alter(self, function):
        """
        Alters the currently stored reference by applying a function on it.

        :param function: (Function), A stateful serializable object which represents the Function defined on
            server side.
            This object must have a serializable Function counter part registered on server side with the actual
            ``org.hazelcast.core.IFunction`` implementation.
        """
        check_not_none(function, "function can't be None")
        return self._encode_invoke(atomic_reference_alter_codec, function=self._to_data(function))

    def apply(self, function):
        """
        Applies a function on the value, the actual stored value will not change.

        :param function: (Function), A stateful serializable object which represents the Function defined on
            server side.
            This object must have a serializable Function counter part registered on server side with the actual
            ``org.hazelcast.core.IFunction`` implementation.
        :return: (object), the result of the function application.
        """
        check_not_none(function, "function can't be None")
        return self._encode_invoke(atomic_reference_apply_codec, function=self._to_data(function))

    def alter_and_get(self, function):
        """
        Alters the currently stored reference by applying a function on it and gets the result.

        :param function: (Function), A stateful serializable object which represents the Function defined on
            server side.
            This object must have a serializable Function counter part registered on server side with the actual
            ``org.hazelcast.core.IFunction`` implementation.
        :return: (object), the new value, the result of the applied function.
        """
        check_not_none(function, "function can't be None")
        return self._encode_invoke(atomic_reference_alter_and_get_codec, function=self._to_data(function))

    def compare_and_set(self, expected, updated):
        """
        Atomically sets the value to the given updated value only if the current value == the expected value.

        :param expected: (object), the expected value.
        :param updated: (object), the new value.
        :return: (bool), ``true`` if successful; or ``false`` if the actual value was not equal to the expected value.
        """
        return self._encode_invoke(atomic_reference_compare_and_set_codec,
                                   expected=self._to_data(expected), updated=self._to_data(updated))

    def clear(self):
        """
        Clears the current stored reference.
        """
        return self._encode_invoke(atomic_reference_clear_codec)

    def contains(self, expected):
        """
        Checks if the reference contains the value.

        :param expected: (object), the value to check (is allowed to be ``None``).
        :return: (bool), ``true`` if the value is found, ``false`` otherwise.
        """

        return self._encode_invoke(atomic_reference_contains_codec,
                                   expected=self._to_data(expected))

    def get(self):
        """
        Gets the current value.

        :return: (object), the current value.
        """
        return self._encode_invoke(atomic_reference_get_codec)

    def get_and_alter(self, function):
        """
        Alters the currently stored reference by applying a function on it on and gets the old value.

        :param function: (Function), A stateful serializable object which represents the Function defined on
            server side.
            This object must have a serializable Function counter part registered on server side with the actual
            ``org.hazelcast.core.IFunction`` implementation.
        :return: (object), the old value, the value before the function is applied.
        """

        check_not_none(function, "function can't be None")
        return self._encode_invoke(atomic_reference_get_and_alter_codec, function=self._to_data(function))

    def get_and_set(self, new_value):
        """
        Gets the old value and sets the new value.

        :param new_value: (object), the new value.
        :return: (object), the old value.
        """
        return self._encode_invoke(atomic_reference_get_and_set_codec,
                                   new_value=self._to_data(new_value))

    def is_null(self):
        """
        Checks if the stored reference is null.

        :return: (bool), ``true`` if null, ``false`` otherwise.
        """
        return self._encode_invoke(atomic_reference_is_null_codec)

    def set(self, new_value):
        """
        Atomically sets the given value.

        :param new_value: (object), the new value.
        """
        return self._encode_invoke(atomic_reference_set_codec,
                                   new_value=self._to_data(new_value))

    def set_and_get(self, new_value):
        """
        Sets and gets the value.

        :param new_value: (object), the new value.
        :return: (object), the new value.
        """
        return self._encode_invoke(atomic_reference_set_and_get_codec,
                                   new_value=self._to_data(new_value))
