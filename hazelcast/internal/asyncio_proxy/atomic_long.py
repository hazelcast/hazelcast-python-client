import typing

from hazelcast.internal.asyncio_proxy.cp import BaseCPProxy
from hazelcast.protocol.codec import (
    atomic_long_add_and_get_codec,
    atomic_long_compare_and_set_codec,
    atomic_long_get_codec,
    atomic_long_get_and_add_codec,
    atomic_long_get_and_set_codec,
    atomic_long_alter_codec,
    atomic_long_apply_codec,
)
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import check_is_int, check_not_none


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

    async def add_and_get(self, delta: int) -> int:
        """Atomically adds the given value to the current value.

        Args:
            delta: The value to add to the current value.

        Returns:
            The updated value, the given value added to the current value.
        """
        check_is_int(delta)
        codec = atomic_long_add_and_get_codec
        request = codec.encode_request(self._group_id, self._object_name, delta)
        return await self._ainvoke(request, codec.decode_response)

    async def compare_and_set(self, expect: int, update: int) -> bool:
        """Atomically sets the value to the given updated value
        only if the current value equals the expected value.

        Args:
            expect: The expected value.
            update: The new value.

        Returns:
            ``True`` if successful; or ``False`` if the actual value was not
            equal to the expected value.
        """
        check_is_int(expect)
        check_is_int(update)
        codec = atomic_long_compare_and_set_codec
        request = codec.encode_request(self._group_id, self._object_name, expect, update)
        return await self._ainvoke(request, codec.decode_response)

    async def decrement_and_get(self) -> int:
        """Atomically decrements the current value by one.

        Returns:
            The updated value, the current value decremented by one.
        """
        return await self.add_and_get(-1)

    async def get_and_decrement(self) -> int:
        """Atomically decrements the current value by one.

        Returns:
            The old value.
        """
        return await self.get_and_add(-1)

    async def get(self) -> int:
        """Gets the current value.

        Returns:
            The current value.
        """
        codec = atomic_long_get_codec
        request = codec.encode_request(self._group_id, self._object_name)
        return await self._ainvoke(request, codec.decode_response)

    async def get_and_add(self, delta: int) -> int:
        """Atomically adds the given value to the current value.

        Args:
            delta: The value to add to the current value.

        Returns:
            The old value before the add.
        """
        check_is_int(delta)
        codec = atomic_long_get_and_add_codec
        request = codec.encode_request(self._group_id, self._object_name, delta)
        return await self._ainvoke(request, codec.decode_response)

    async def get_and_set(self, new_value: int) -> int:
        """Atomically sets the given value and returns the old value.

        Args:
            new_value: The new value.

        Returns:
            The old value.
        """
        check_is_int(new_value)
        codec = atomic_long_get_and_set_codec
        request = codec.encode_request(self._group_id, self._object_name, new_value)
        return await self._ainvoke(request, codec.decode_response)

    async def increment_and_get(self) -> int:
        """Atomically increments the current value by one.

        Returns:
            The updated value, the current value incremented by one.
        """
        return await self.add_and_get(1)

    async def get_and_increment(self) -> int:
        """Atomically increments the current value by one.

        Returns:
            The old value.
        """
        return await self.get_and_add(1)

    async def set(self, new_value: int) -> None:
        """Atomically sets the given value.

        Args:
            new_value: The new value
        """
        check_is_int(new_value)
        codec = atomic_long_get_and_set_codec
        request = codec.encode_request(self._group_id, self._object_name, new_value)
        return await self._ainvoke(request)

    async def alter(self, function: typing.Any) -> None:
        """Alters the currently stored value by applying a function on it.

        Notes:
            ``function`` must be an instance of Hazelcast serializable type.
            It must have a counterpart registered in the server-side that
            implements the ``com.hazelcast.core.IFunction`` interface with
            the actual logic of the function to be applied.

        Args:
            function: The function that alters the currently stored value.
        """
        check_not_none(function, "Function cannot be None")
        try:
            function_data = self._to_data(function)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.alter, function)

        codec = atomic_long_alter_codec
        # 1 means return the new value.
        # There is no way to tell server to return nothing as of now (30.09.2020)
        # The new value is `long` (comes with the initial frame) and we
        # don't try to decode it. So, this shouldn't cause any problems.
        request = codec.encode_request(self._group_id, self._object_name, function_data, 1)
        return await self._ainvoke(request)

    async def alter_and_get(self, function: typing.Any) -> int:
        """Alters the currently stored value by applying a function on it and
        gets the result.

        Notes:
            ``function`` must be an instance of Hazelcast serializable type.
            It must have a counterpart registered in the server-side that
            implements the ``com.hazelcast.core.IFunction`` interface with
            the actual logic of the function to be applied.

        Args:
            function: The function that alters the currently stored value.

        Returns:
            The new value.
        """
        check_not_none(function, "Function cannot be None")
        try:
            function_data = self._to_data(function)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.alter_and_get, function)

        codec = atomic_long_alter_codec
        # 1 means return the new value.
        request = codec.encode_request(self._group_id, self._object_name, function_data, 1)
        return await self._ainvoke(request, codec.decode_response)

    async def get_and_alter(self, function: typing.Any) -> int:
        """Alters the currently stored value by applying a function on it and
        gets the old value.

        Notes:
            ``function`` must be an instance of Hazelcast serializable type.
            It must have a counterpart registered in the server-side that
            implements the ``com.hazelcast.core.IFunction`` interface with
            the actual logic of the function to be applied.

        Args:
            function: The function that alters the currently stored value.

        Returns:
            The old value.
        """
        check_not_none(function, "Function cannot be None")
        try:
            function_data = self._to_data(function)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.get_and_alter, function)

        codec = atomic_long_alter_codec
        # 0 means return the old value.
        request = codec.encode_request(self._group_id, self._object_name, function_data, 0)
        return await self._ainvoke(request, codec.decode_response)

    async def apply(self, function: typing.Any) -> typing.Any:
        """Applies a function on the value, the actual stored value will not
        change.

        Notes:
            ``function`` must be an instance of Hazelcast serializable type.
            It must have a counterpart registered in the server-side that
            implements the ``com.hazelcast.core.IFunction`` interface with
            the actual logic of the function to be applied.

        Args:
            function: The function applied to the currently stored value.

        Returns:
            The result of the function application.
        """
        check_not_none(function, "Function cannot be None")
        try:
            function_data = self._to_data(function)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.apply, function)

        codec = atomic_long_apply_codec
        request = codec.encode_request(self._group_id, self._object_name, function_data)

        def handler(response):
            return self._to_object(codec.decode_response(response))

        return await self._ainvoke(request, handler)
