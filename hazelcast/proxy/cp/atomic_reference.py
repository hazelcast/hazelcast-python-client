import typing

from hazelcast.future import Future
from hazelcast.protocol.codec import (
    atomic_ref_compare_and_set_codec,
    atomic_ref_get_codec,
    atomic_ref_set_codec,
    atomic_ref_contains_codec,
    atomic_ref_apply_codec,
)
from hazelcast.proxy.cp import BaseCPProxy
from hazelcast.types import ElementType
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import check_not_none


class AtomicReference(BaseCPProxy["BlockingAtomicReference"], typing.Generic[ElementType]):
    """A distributed, highly available object reference with atomic operations.

    AtomicReference offers linearizability during crash failures and network
    partitions. It is CP with respect to the CAP principle. If a network
    partition occurs, it remains available on at most one side of the
    partition.

    The following are some considerations you need to know when you use
    AtomicReference:

    - AtomicReference works based on the byte-content and not on the
      object-reference. If you use the ``compare_and_set()`` method, do not
      change the original value because its serialized content will then be
      different.
    - All methods returning an object return a private copy. You can modify the
      private copy, but the rest of the world is shielded from your changes. If
      you want these changes to be visible to the rest of the world, you need
      to write the change back to the AtomicReference; but be careful about
      introducing a data-race.
    - The in-memory format of an AtomicReference is ``binary``. The receiving
      side does not need to have the class definition available unless it needs
      to be deserialized on the other side., e.g., because a method like
      `alter()` is executed. This deserialization is done for every call that
      needs to have the object instead of the binary content, so be careful
      with expensive object graphs that need to be deserialized.
    - If you have an object with many fields or an object graph, and you only
      need to calculate some information or need a subset of fields, you can
      use the `apply()` method. With the `apply()` method, the whole object
      does not need to be sent over the line; only the information that is
      relevant is sent.

    IAtomicReference does not offer exactly-once / effectively-once
    execution semantics. It goes with at-least-once execution semantics
    by default and can cause an API call to be committed multiple times
    in case of CP member failures. It can be tuned to offer at-most-once
    execution semantics. Please see `fail-on-indeterminate-operation-state`
    server-side setting.
    """

    def compare_and_set(
        self, expect: typing.Optional[ElementType], update: typing.Optional[ElementType]
    ) -> Future[bool]:
        """Atomically sets the value to the given updated value
        only if the current value is equal to the expected value.

        Args:
            expect: The expected value.
            update: The new value.

        Returns:
            ``True`` if successful, or ``False`` if the actual value was not
            equal to the expected value.
        """
        try:
            expected_data = self._to_data(expect)
            new_data = self._to_data(update)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.compare_and_set, expect, update)

        codec = atomic_ref_compare_and_set_codec
        request = codec.encode_request(self._group_id, self._object_name, expected_data, new_data)
        return self._invoke(request, codec.decode_response)

    def get(self) -> Future[typing.Optional[ElementType]]:
        """Gets the current value.

        Returns:
            The current value.
        """
        codec = atomic_ref_get_codec
        request = codec.encode_request(self._group_id, self._object_name)

        def handler(response):
            return self._to_object(codec.decode_response(response))

        return self._invoke(request, handler)

    def set(self, new_value: typing.Optional[ElementType]) -> Future[None]:
        """Atomically sets the given value.

        Args:
            new_value: The new value.
        """
        try:
            new_value_data = self._to_data(new_value)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.set, new_value)

        codec = atomic_ref_set_codec
        request = codec.encode_request(self._group_id, self._object_name, new_value_data, False)
        return self._invoke(request)

    def get_and_set(
        self, new_value: typing.Optional[ElementType]
    ) -> Future[typing.Optional[ElementType]]:
        """Gets the old value and sets the new value.

        Args:
            new_value: The new value.

        Returns:
            The old value.
        """
        try:
            new_value_data = self._to_data(new_value)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.get_and_set, new_value)

        codec = atomic_ref_set_codec
        request = codec.encode_request(self._group_id, self._object_name, new_value_data, True)

        def handler(response):
            return self._to_object(codec.decode_response(response))

        return self._invoke(request, handler)

    def is_none(self) -> Future[bool]:
        """Checks if the stored reference is ``None``.

        Returns:
            ``True`` if the stored reference is ``None``, ``False`` otherwise.
        """
        return self.contains(None)

    def clear(self) -> Future[None]:
        """Clears the current stored reference, so it becomes ``None``."""
        return self.set(None)

    def contains(self, value: typing.Optional[ElementType]) -> Future[bool]:
        """Checks if the reference contains the value.

        Args:
            value: The value to check (is allowed to be ``None``).

        Returns:
            ``True`` if the value is found, ``False`` otherwise.
        """
        try:
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.contains, value)

        codec = atomic_ref_contains_codec
        request = codec.encode_request(self._group_id, self._object_name, value_data)
        return self._invoke(request, codec.decode_response)

    def alter(self, function: typing.Any) -> Future[None]:
        """Alters the currently stored reference by applying a function on it.

        Notes:
            ``function`` must be an instance of Hazelcast serializable type.
            It must have a counterpart registered in the server-side that
            implements the ``com.hazelcast.core.IFunction`` interface with
            the actual logic of the function to be applied.

        Args:
            function: The function that alters the currently stored reference.
        """
        check_not_none(function, "Function cannot be None")
        try:
            function_data = self._to_data(function)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.alter, function)

        codec = atomic_ref_apply_codec
        # 0 means don't return the value
        request = codec.encode_request(self._group_id, self._object_name, function_data, 0, True)
        return self._invoke(request)

    def alter_and_get(self, function: typing.Any) -> Future[typing.Optional[ElementType]]:
        """Alters the currently stored reference by applying a function on it
        and gets the result.

        Notes:
            ``function`` must be an instance of Hazelcast serializable type.
            It must have a counterpart registered in the server-side that
            implements the ``com.hazelcast.core.IFunction`` interface with
            the actual logic of the function to be applied.

        Args:
            function: The function that alters the currently stored reference.

        Returns:
            The new value, the result of the applied function.
        """
        check_not_none(function, "Function cannot be None")
        try:
            function_data = self._to_data(function)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.alter_and_get, function)

        codec = atomic_ref_apply_codec
        # 2 means return the new value
        request = codec.encode_request(self._group_id, self._object_name, function_data, 2, True)

        def handler(response):
            return self._to_object(codec.decode_response(response))

        return self._invoke(request, handler)

    def get_and_alter(self, function: typing.Any) -> Future[typing.Optional[ElementType]]:
        """Alters the currently stored reference by applying a function on it
        on and gets the old value.

        Notes:
            ``function`` must be an instance of Hazelcast serializable type.
            It must have a counterpart registered in the server-side that
            implements the ``com.hazelcast.core.IFunction`` interface with
            the actual logic of the function to be applied.

        Args:
            function: The function that alters the currently stored reference.

        Returns:
            The old value, the value before the function is applied.
        """
        check_not_none(function, "Function cannot be None")
        try:
            function_data = self._to_data(function)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.get_and_alter, function)

        codec = atomic_ref_apply_codec
        # 1 means return the old value
        request = codec.encode_request(self._group_id, self._object_name, function_data, 1, True)

        def handler(response):
            return self._to_object(codec.decode_response(response))

        return self._invoke(request, handler)

    def apply(self, function: typing.Any) -> Future[typing.Optional[ElementType]]:
        """Applies a function on the value, the actual stored value will not
        change.

        Notes:
            ``function`` must be an instance of Hazelcast serializable type.
            It must have a counterpart registered in the server-side that
            implements the ``com.hazelcast.core.IFunction`` interface with
            the actual logic of the function to be applied.

        Args:
            function: The function applied on the currently stored reference.

        Returns:
            The result of the function application.
        """
        check_not_none(function, "Function cannot be None")
        try:
            function_data = self._to_data(function)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.apply, function)

        codec = atomic_ref_apply_codec
        # 2 means return the new value
        request = codec.encode_request(self._group_id, self._object_name, function_data, 2, False)

        def handler(response):
            return self._to_object(codec.decode_response(response))

        return self._invoke(request, handler)

    def blocking(self) -> "BlockingAtomicReference[ElementType]":
        return BlockingAtomicReference(self)


class BlockingAtomicReference(AtomicReference[ElementType]):
    __slots__ = ("_wrapped",)

    def __init__(self, wrapped: AtomicReference[ElementType]):
        self._wrapped = wrapped

    def compare_and_set(  # type: ignore[override]
        self,
        expect: typing.Optional[ElementType],
        update: typing.Optional[ElementType],
    ) -> bool:
        return self._wrapped.compare_and_set(expect, update).result()

    def get(  # type: ignore[override]
        self,
    ) -> typing.Optional[ElementType]:
        return self._wrapped.get().result()

    def set(  # type: ignore[override]
        self,
        new_value: typing.Optional[ElementType],
    ) -> None:
        return self._wrapped.set(new_value).result()

    def get_and_set(  # type: ignore[override]
        self,
        new_value: typing.Optional[ElementType],
    ) -> typing.Optional[ElementType]:
        return self._wrapped.get_and_set(new_value).result()

    def is_none(  # type: ignore[override]
        self,
    ) -> bool:
        return self._wrapped.is_none().result()

    def clear(  # type: ignore[override]
        self,
    ) -> None:
        return self._wrapped.clear().result()

    def contains(  # type: ignore[override]
        self,
        value: typing.Optional[ElementType],
    ) -> bool:
        return self._wrapped.contains(value).result()

    def alter(  # type: ignore[override]
        self,
        function: typing.Any,
    ) -> None:
        return self._wrapped.alter(function).result()

    def alter_and_get(  # type: ignore[override]
        self,
        function: typing.Any,
    ) -> typing.Optional[ElementType]:
        return self._wrapped.alter_and_get(function).result()

    def get_and_alter(  # type: ignore[override]
        self,
        function: typing.Any,
    ) -> typing.Optional[ElementType]:
        return self._wrapped.get_and_alter(function).result()

    def apply(  # type: ignore[override]
        self,
        function: typing.Any,
    ) -> typing.Optional[ElementType]:
        return self._wrapped.apply(function).result()

    def destroy(  # type: ignore[override]
        self,
    ) -> None:
        return self._wrapped.destroy().result()

    def blocking(self) -> "BlockingAtomicReference[ElementType]":
        return self

    def __repr__(self) -> str:
        return self._wrapped.__repr__()
