from hazelcast.protocol.codec import (
    atomic_long_add_and_get_codec,
    atomic_long_compare_and_set_codec,
    atomic_long_get_codec,
    atomic_long_get_and_add_codec,
    atomic_long_get_and_set_codec,
    atomic_long_alter_codec,
    atomic_long_apply_codec,
)
from hazelcast.proxy.cp import BaseCPProxy
from hazelcast.util import check_not_none, check_is_int


class AtomicLong(BaseCPProxy):
    """AtomicLong is a redundant and highly available distributed counter
    for 64-bit integers (``long`` type in Java).

    It works on top of the Raft consensus algorithm. It offers linearizability
    during crash failures and network partitions. It is CP with respect to
    the CAP principle. If a network partition occurs, it remains available
    on at most one side of the partition.

    AtomicLong implementation does not offer exactly-once / effectively-once
    execution semantics. It goes with at-least-once execution semantics
    by default and can cause an API call to be committed multiple times
    in case of CP member failures. It can be tuned to offer at-most-once
    execution semantics. Please see `fail-on-indeterminate-operation-state`
    server-side setting.
    """

    def add_and_get(self, delta):
        """Atomically adds the given value to the current value.

        Args:
            delta (int): The value to add to the current value.

        Returns:
            hazelcast.future.Future[int]: The updated value, the given value added
            to the current value
        """
        check_is_int(delta)
        codec = atomic_long_add_and_get_codec
        request = codec.encode_request(self._group_id, self._object_name, delta)
        return self._invoke(request, codec.decode_response)

    def compare_and_set(self, expect, update):
        """Atomically sets the value to the given updated value
        only if the current value equals the expected value.

        Args:
            expect (int): The expected value.
            update (int): The new value.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if successful; or ``False`` if
            the actual value was not equal to the expected value.
        """
        check_is_int(expect)
        check_is_int(update)
        codec = atomic_long_compare_and_set_codec
        request = codec.encode_request(self._group_id, self._object_name, expect, update)
        return self._invoke(request, codec.decode_response)

    def decrement_and_get(self):
        """Atomically decrements the current value by one.

        Returns:
            hazelcast.future.Future[int]: The updated value, the current value
            decremented by one.
        """
        return self.add_and_get(-1)

    def get_and_decrement(self):
        """Atomically decrements the current value by one.

        Returns:
            hazelcast.future.Future[int]: The old value.
        """
        return self.get_and_add(-1)

    def get(self):
        """Gets the current value.

        Returns:
            hazelcast.future.Future[int]: The current value.
        """
        codec = atomic_long_get_codec
        request = codec.encode_request(self._group_id, self._object_name)
        return self._invoke(request, codec.decode_response)

    def get_and_add(self, delta):
        """Atomically adds the given value to the current value.

        Args:
            delta (int): The value to add to the current value.

        Returns:
            hazelcast.future.Future[int]: The old value before the add.
        """
        check_is_int(delta)
        codec = atomic_long_get_and_add_codec
        request = codec.encode_request(self._group_id, self._object_name, delta)
        return self._invoke(request, codec.decode_response)

    def get_and_set(self, new_value):
        """Atomically sets the given value and returns the old value.

        Args:
            new_value (int): The new value.

        Returns:
            hazelcast.future.Future[int]: The old value.
        """
        check_is_int(new_value)
        codec = atomic_long_get_and_set_codec
        request = codec.encode_request(self._group_id, self._object_name, new_value)
        return self._invoke(request, codec.decode_response)

    def increment_and_get(self):
        """Atomically increments the current value by one.

        Returns:
            hazelcast.future.Future[int]: The updated value, the current value
            incremented by one.
        """
        return self.add_and_get(1)

    def get_and_increment(self):
        """Atomically increments the current value by one.

        Returns:
            hazelcast.future.Future[int]: The old value.
        """
        return self.get_and_add(1)

    def set(self, new_value):
        """Atomically sets the given value.

        Args:
            new_value (int): The new value

        Returns:
            hazelcast.future.Future[None]:
        """
        check_is_int(new_value)
        codec = atomic_long_get_and_set_codec
        request = codec.encode_request(self._group_id, self._object_name, new_value)
        return self._invoke(request)

    def alter(self, function):
        """Alters the currently stored value by applying a function on it.

        Notes:
            ``function`` must be an instance of ``IdentifiedDataSerializable`` or
            ``Portable`` that has a counterpart that implements the
            ``com.hazelcast.core.IFunction`` interface registered on the server-side with
            the actual implementation of the function to be applied.

        Args:
            function (hazelcast.serialization.api.Portable or hazelcast.serialization.api.IdentifiedDataSerializable):
                The function that alters the currently stored value.

        Returns:
            hazelcast.future.Future[None]:
        """
        check_not_none(function, "Function cannot be None")
        function_data = self._to_data(function)
        codec = atomic_long_alter_codec
        # 1 means return the new value.
        # There is no way to tell server to return nothing as of now (30.09.2020)
        # The new value is `long` (comes with the initial frame) and we
        # don't try to decode it. So, this shouldn't cause any problems.
        request = codec.encode_request(self._group_id, self._object_name, function_data, 1)
        return self._invoke(request)

    def alter_and_get(self, function):
        """Alters the currently stored value by applying a function on it and gets the result.

        Notes:
            ``function`` must be an instance of ``IdentifiedDataSerializable`` or
            ``Portable`` that has a counterpart that implements the
            ``com.hazelcast.core.IFunction`` interface registered on the server-side with
            the actual implementation of the function to be applied.

        Args:
            function (hazelcast.serialization.api.Portable or hazelcast.serialization.api.IdentifiedDataSerializable):
                The function that alters the currently stored value.

        Returns:
            hazelcast.future.Future[int]: The new value.
        """
        check_not_none(function, "Function cannot be None")
        function_data = self._to_data(function)
        codec = atomic_long_alter_codec
        # 1 means return the new value.
        request = codec.encode_request(self._group_id, self._object_name, function_data, 1)
        return self._invoke(request, codec.decode_response)

    def get_and_alter(self, function):
        """Alters the currently stored value by applying a function on it on and gets the old value.

        Notes:
            ``function`` must be an instance of ``IdentifiedDataSerializable`` or
            ``Portable`` that has a counterpart that implements the
            ``com.hazelcast.core.IFunction`` interface registered on the server-side with
            the actual implementation of the function to be applied.

        Args:
            function (hazelcast.serialization.api.Portable or hazelcast.serialization.api.IdentifiedDataSerializable):
                The function that alters the currently stored value.

        Returns:
            hazelcast.future.Future[int]: The old value.
        """
        check_not_none(function, "Function cannot be None")
        function_data = self._to_data(function)
        codec = atomic_long_alter_codec
        # 0 means return the old value.
        request = codec.encode_request(self._group_id, self._object_name, function_data, 0)
        return self._invoke(request, codec.decode_response)

    def apply(self, function):
        """Applies a function on the value, the actual stored value will not change.

        Notes:
            ``function`` must be an instance of ``IdentifiedDataSerializable`` or
            ``Portable`` that has a counterpart that implements the
            ``com.hazelcast.core.IFunction`` interface registered on the server-side with
            the actual implementation of the function to be applied.

        Args:
            function (hazelcast.serialization.api.Portable or hazelcast.serialization.api.IdentifiedDataSerializable):
                The function applied to the currently stored value.

        Returns:
            hazelcast.future.Future[any]: The result of the function application.
        """
        check_not_none(function, "Function cannot be None")
        function_data = self._to_data(function)
        codec = atomic_long_apply_codec
        request = codec.encode_request(self._group_id, self._object_name, function_data)

        def handler(response):
            return self._to_object(codec.decode_response(response))

        return self._invoke(request, handler)
