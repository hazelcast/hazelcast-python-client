from hazelcast.protocol.codec import atomic_long_add_and_get_codec, atomic_long_compare_and_set_codec, \
    atomic_long_decrement_and_get_codec, atomic_long_get_and_add_codec, atomic_long_get_and_increment_codec, \
    atomic_long_get_and_set_codec, atomic_long_get_codec, atomic_long_increment_and_get_codec, atomic_long_set_codec, \
    atomic_long_alter_and_get_codec, atomic_long_alter_codec, atomic_long_apply_codec, atomic_long_get_and_alter_codec
from hazelcast.proxy.base import PartitionSpecificProxy
from hazelcast.util import check_not_none


class AtomicLong(PartitionSpecificProxy):
    """
    AtomicLong is a redundant and highly available distributed long value which can be updated atomically.
    """
    def add_and_get(self, delta):
        """
        Atomically adds the given delta value to the currently stored value.

        :param delta: (long), the value to add to the currently stored value.
        :return: (long), the updated value.
        """
        return self._encode_invoke(atomic_long_add_and_get_codec, delta=delta)

    def alter(self, function):
        """
        Alters the currently stored value by applying a function on it.

        :param function: (Function), A stateful serializable object which represents the Function defined on
            server side.
            This object must have a serializable Function counter part registered on server side with the actual
            ``org.hazelcast.core.IFunction`` implementation.
        """
        check_not_none(function, "function can't be None")
        return self._encode_invoke(atomic_long_alter_codec, function=self._to_data(function))

    def alter_and_get(self, function):
        """
        Alters the currently stored value by applying a function on it and gets the result.

        :param function: (Function), A stateful serializable object which represents the Function defined on
            server side.
            This object must have a serializable Function counter part registered on server side with the actual
            ``org.hazelcast.core.IFunction`` implementation.
        :return: (long), the new value.
        """
        check_not_none(function, "function can't be None")
        return self._encode_invoke(atomic_long_alter_and_get_codec, function=self._to_data(function))

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
        return self._encode_invoke(atomic_long_apply_codec, function=self._to_data(function))

    def compare_and_set(self, expected, updated):
        """
        Atomically sets the value to the given updated value only if the current value == the expected value.

        :param expected: (long), the expected value.
        :param updated: (long), the new value.
        :return: (bool), ``true`` if successful; or ``false`` if the actual value was not equal to the expected value.
        """
        return self._encode_invoke(atomic_long_compare_and_set_codec, expected=expected,
                                   updated=updated)

    def decrement_and_get(self):
        """
        Atomically decrements the current value by one.

        :return: (long), the updated value, the current value decremented by one.
        """
        return self._encode_invoke(atomic_long_decrement_and_get_codec)

    def get(self):
        """
        Gets the current value.

        :return: (long), gets the current value.
        """
        return self._encode_invoke(atomic_long_get_codec)

    def get_and_add(self, delta):
        """
        Atomically adds the given value to the current value.

        :param delta: (long), the value to add to the current value.
        :return: (long), the old value before the addition.
        """
        return self._encode_invoke(atomic_long_get_and_add_codec, delta=delta)

    def get_and_alter(self, function):
        """
        Alters the currently stored value by applying a function on it on and gets the old value.

        :param function: (Function), A stateful serializable object which represents the Function defined on
            server side.
            This object must have a serializable Function counter part registered on server side with the actual
            ``org.hazelcast.core.IFunction`` implementation.
        :return: (long), the old value.
        """
        check_not_none(function, "function can't be None")
        return self._encode_invoke(atomic_long_get_and_alter_codec, function=self._to_data(function))

    def get_and_set(self, new_value):
        """
        Atomically sets the given value and returns the old value.

        :param new_value: (long), the new value.
        :return: (long), the old value.
        """
        return self._encode_invoke(atomic_long_get_and_set_codec, new_value=new_value)

    def increment_and_get(self):
        """
        Atomically increments the current value by one.

        :return: (long), the updated value, the current value incremented by one.
        """
        return self._encode_invoke(atomic_long_increment_and_get_codec)

    def get_and_increment(self):
        """
        Atomically increments the current value by one.

        :return: (long), the old value.
        """
        return self._encode_invoke(atomic_long_get_and_increment_codec)

    def set(self, new_value):
        """
        Atomically sets the given value.

        :param new_value: (long), the new value.
        """
        return self._encode_invoke(atomic_long_set_codec, new_value=new_value)
