import typing

from hazelcast.future import Future
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
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import check_not_none, check_is_int


class AtomicLong(BaseCPProxy["BlockingAtomicLong"]):
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

    def add_and_get(self, delta: int) -> Future[int]:
        """Atomically adds the given value to the current value.

        Args:
            delta: The value to add to the current value.

        Returns:
            The updated value, the given value added to the current value.
        """
        check_is_int(delta)
        codec = atomic_long_add_and_get_codec
        request = codec.encode_request(self._group_id, self._object_name, delta)
        return self._invoke(request, codec.decode_response)

    def compare_and_set(self, expect: int, update: int) -> Future[bool]:
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
        return self._invoke(request, codec.decode_response)

    def decrement_and_get(self) -> Future[int]:
        """Atomically decrements the current value by one.

        Returns:
            The updated value, the current value decremented by one.
        """
        return self.add_and_get(-1)

    def get_and_decrement(self) -> Future[int]:
        """Atomically decrements the current value by one.

        Returns:
            The old value.
        """
        return self.get_and_add(-1)

    def get(self) -> Future[int]:
        """Gets the current value.

        Returns:
            The current value.
        """
        codec = atomic_long_get_codec
        request = codec.encode_request(self._group_id, self._object_name)
        return self._invoke(request, codec.decode_response)

    def get_and_add(self, delta: int) -> Future[int]:
        """Atomically adds the given value to the current value.

        Args:
            delta: The value to add to the current value.

        Returns:
            The old value before the add.
        """
        check_is_int(delta)
        codec = atomic_long_get_and_add_codec
        request = codec.encode_request(self._group_id, self._object_name, delta)
        return self._invoke(request, codec.decode_response)

    def get_and_set(self, new_value: int) -> Future[int]:
        """Atomically sets the given value and returns the old value.

        Args:
            new_value: The new value.

        Returns:
            The old value.
        """
        check_is_int(new_value)
        codec = atomic_long_get_and_set_codec
        request = codec.encode_request(self._group_id, self._object_name, new_value)
        return self._invoke(request, codec.decode_response)

    def increment_and_get(self) -> Future[int]:
        """Atomically increments the current value by one.

        Returns:
            The updated value, the current value incremented by one.
        """
        return self.add_and_get(1)

    def get_and_increment(self) -> Future[int]:
        """Atomically increments the current value by one.

        Returns:
            The old value.
        """
        return self.get_and_add(1)

    def set(self, new_value: int) -> Future[None]:
        """Atomically sets the given value.

        Args:
            new_value: The new value
        """
        check_is_int(new_value)
        codec = atomic_long_get_and_set_codec
        request = codec.encode_request(self._group_id, self._object_name, new_value)
        return self._invoke(request)

    def alter(self, function: typing.Any) -> Future[None]:
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
            return self._send_schema_and_retry(e, self.alter, function)

        codec = atomic_long_alter_codec
        # 1 means return the new value.
        # There is no way to tell server to return nothing as of now (30.09.2020)
        # The new value is `long` (comes with the initial frame) and we
        # don't try to decode it. So, this shouldn't cause any problems.
        request = codec.encode_request(self._group_id, self._object_name, function_data, 1)
        return self._invoke(request)

    def alter_and_get(self, function: typing.Any) -> Future[int]:
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
            return self._send_schema_and_retry(e, self.alter_and_get, function)

        codec = atomic_long_alter_codec
        # 1 means return the new value.
        request = codec.encode_request(self._group_id, self._object_name, function_data, 1)
        return self._invoke(request, codec.decode_response)

    def get_and_alter(self, function: typing.Any) -> Future[int]:
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
            return self._send_schema_and_retry(e, self.get_and_alter, function)

        codec = atomic_long_alter_codec
        # 0 means return the old value.
        request = codec.encode_request(self._group_id, self._object_name, function_data, 0)
        return self._invoke(request, codec.decode_response)

    def apply(self, function: typing.Any) -> Future[typing.Any]:
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

        return self._invoke(request, handler)

    def blocking(self) -> "BlockingAtomicLong":
        return BlockingAtomicLong(self)


class BlockingAtomicLong(AtomicLong):
    __slots__ = ("_wrapped",)

    def __init__(self, wrapped: AtomicLong):
        self._wrapped = wrapped

    def add_and_get(  # type: ignore[override]
        self,
        delta: int,
    ) -> int:
        return self._wrapped.add_and_get(delta).result()

    def compare_and_set(  # type: ignore[override]
        self,
        expect: int,
        update: int,
    ) -> bool:
        return self._wrapped.compare_and_set(expect, update).result()

    def decrement_and_get(  # type: ignore[override]
        self,
    ) -> int:
        return self._wrapped.decrement_and_get().result()

    def get_and_decrement(  # type: ignore[override]
        self,
    ) -> int:
        return self._wrapped.get_and_decrement().result()

    def get(  # type: ignore[override]
        self,
    ) -> int:
        return self._wrapped.get().result()

    def get_and_add(  # type: ignore[override]
        self,
        delta: int,
    ) -> int:
        return self._wrapped.get_and_add(delta).result()

    def get_and_set(  # type: ignore[override]
        self,
        new_value: int,
    ) -> int:
        return self._wrapped.get_and_set(new_value).result()

    def increment_and_get(  # type: ignore[override]
        self,
    ) -> int:
        return self._wrapped.increment_and_get().result()

    def get_and_increment(  # type: ignore[override]
        self,
    ) -> int:
        return self._wrapped.get_and_increment().result()

    def set(  # type: ignore[override]
        self,
        new_value: int,
    ) -> None:
        return self._wrapped.set(new_value).result()

    def alter(  # type: ignore[override]
        self,
        function: typing.Any,
    ) -> None:
        return self._wrapped.alter(function).result()

    def alter_and_get(  # type: ignore[override]
        self,
        function: typing.Any,
    ) -> int:
        return self._wrapped.alter_and_get(function).result()

    def get_and_alter(  # type: ignore[override]
        self,
        function: typing.Any,
    ) -> int:
        return self._wrapped.get_and_alter(function).result()

    def apply(  # type: ignore[override]
        self,
        function: typing.Any,
    ) -> typing.Any:
        return self._wrapped.apply(function).result()

    def destroy(  # type: ignore[override]
        self,
    ) -> None:
        return self._wrapped.destroy().result()

    def blocking(self) -> "BlockingAtomicLong":
        return self

    def __repr__(self) -> str:
        return self._wrapped.__repr__()
