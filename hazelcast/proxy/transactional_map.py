import typing

from hazelcast.predicate import Predicate
from hazelcast.protocol.codec import (
    transactional_map_contains_key_codec,
    transactional_map_delete_codec,
    transactional_map_get_codec,
    transactional_map_get_for_update_codec,
    transactional_map_is_empty_codec,
    transactional_map_key_set_codec,
    transactional_map_key_set_with_predicate_codec,
    transactional_map_put_codec,
    transactional_map_put_if_absent_codec,
    transactional_map_remove_codec,
    transactional_map_remove_if_same_codec,
    transactional_map_replace_codec,
    transactional_map_replace_if_same_codec,
    transactional_map_set_codec,
    transactional_map_size_codec,
    transactional_map_values_codec,
    transactional_map_values_with_predicate_codec,
)
from hazelcast.proxy.base import TransactionalProxy
from hazelcast.types import ValueType, KeyType
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import check_not_none, to_millis, thread_id, ImmutableLazyDataList


class TransactionalMap(TransactionalProxy, typing.Generic[KeyType, ValueType]):
    """Transactional implementation of :class:`~hazelcast.proxy.map.Map`."""

    def contains_key(self, key: KeyType) -> bool:
        """Transactional implementation of
        :func:`Map.contains_key(key) <hazelcast.proxy.map.Map.contains_key>`

        Args:
            key: The specified key.

        Returns:
            ``True`` if this map contains an entry for the specified key,
            ``False`` otherwise.
        """
        check_not_none(key, "key can't be none")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.contains_key(key)

        request = transactional_map_contains_key_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data
        )
        return self._invoke(request, transactional_map_contains_key_codec.decode_response)

    def get(self, key: KeyType) -> typing.Optional[ValueType]:
        """Transactional implementation of
        :func:`Map.get(key) <hazelcast.proxy.map.Map.get>`

        Args:
            key: The specified key.

        Returns:
            The value for the specified key.
        """
        check_not_none(key, "key can't be none")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.get(key)

        def handler(message):
            return self._to_object(transactional_map_get_codec.decode_response(message))

        request = transactional_map_get_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data
        )
        return self._invoke(request, handler)

    def get_for_update(self, key: KeyType) -> typing.Optional[ValueType]:
        """Locks the key and then gets and returns the value to which the
        specified key is mapped.

        Lock will be released at the end of the transaction (either commit or
        rollback).

        Args:
            key: The specified key.

        Returns:
            The value for the specified key.

        See Also:
            :func:`Map.get(key) <hazelcast.proxy.map.Map.get>`
        """
        check_not_none(key, "key can't be none")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.get_for_update(key)

        def handler(message):
            return self._to_object(transactional_map_get_for_update_codec.decode_response(message))

        request = transactional_map_get_for_update_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data
        )
        return self._invoke(request, handler)

    def size(self) -> int:
        """Transactional implementation of
        :func:`Map.size() <hazelcast.proxy.map.Map.size>`

        Returns:
            Number of entries in this map.
        """
        request = transactional_map_size_codec.encode_request(
            self.name, self.transaction.id, thread_id()
        )
        return self._invoke(request, transactional_map_size_codec.decode_response)

    def is_empty(self) -> bool:
        """Transactional implementation of
        :func:`Map.is_empty() <hazelcast.proxy.map.Map.is_empty>`

        Returns:
            ``True`` if this map contains no key-value mappings, ``False``
            otherwise.
        """
        request = transactional_map_is_empty_codec.encode_request(
            self.name, self.transaction.id, thread_id()
        )
        return self._invoke(request, transactional_map_is_empty_codec.decode_response)

    def put(self, key: KeyType, value: ValueType, ttl: float = None) -> typing.Optional[ValueType]:
        """Transactional implementation of
        :func:`Map.put(key, value, ttl) <hazelcast.proxy.map.Map.put>`

        The object to be put will be accessible only in the current transaction
        context till the transaction is committed.

        Args:
            key: The specified key.
            value: The value to associate with the key.
            ttl: Maximum time in seconds for this entry to stay.

        Returns:
            Previous value associated with key or ``None`` if there was no
            mapping for key.
        """
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.put(key, value, ttl)

        def handler(message):
            return self._to_object(transactional_map_put_codec.decode_response(message))

        request = transactional_map_put_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data, value_data, to_millis(ttl)
        )
        return self._invoke(request, handler)

    def put_if_absent(self, key: KeyType, value: ValueType) -> typing.Optional[ValueType]:
        """Transactional implementation of
        :func:`Map.put_if_absent(key, value) <hazelcast.proxy.map.Map.put_if_absent>`

        The object to be put will be accessible only in the current transaction
        context till the transaction is committed.

        Args:
            key: Key of the entry.
            value: Value of the entry.

        Returns:
            Old value of the entry.
        """
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.put_if_absent(key, value)

        def handler(message):
            return self._to_object(transactional_map_put_if_absent_codec.decode_response(message))

        request = transactional_map_put_if_absent_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data, value_data
        )
        return self._invoke(request, handler)

    def set(self, key: KeyType, value: ValueType) -> None:
        """Transactional implementation of
        :func:`Map.set(key, value) <hazelcast.proxy.map.Map.set>`

        The object to be set will be accessible only in the current transaction
        context till the transaction is committed.

        Args:
            key: Key of the entry.
            value: Value of the entry.
        """
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.set(key, value)

        request = transactional_map_set_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data, value_data
        )
        return self._invoke(request)

    def replace(self, key: KeyType, value: ValueType) -> typing.Optional[ValueType]:
        """Transactional implementation of
        :func:`Map.replace(key, value) <hazelcast.proxy.map.Map.replace>`

        The object to be replaced will be accessible only in the current
        transaction context till the transaction is committed.

        Args:
            key: The specified key.
            value: The value to replace the previous value.

        Returns:
            Previous value associated with key, or ``None`` if there was no
            mapping for key.
        """
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.replace(key, value)

        def handler(message):
            return self._to_object(transactional_map_replace_codec.decode_response(message))

        request = transactional_map_replace_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data, value_data
        )
        return self._invoke(request, handler)

    def replace_if_same(self, key: KeyType, old_value: ValueType, new_value: ValueType) -> bool:
        """Transactional implementation of
        :func:`Map.replace_if_same(key, old_value, new_value)
        <hazelcast.proxy.map.Map.replace_if_same>`

        The object to be replaced will be accessible only in the current
        transaction context till the transaction is committed.

        Args:
            key: The specified key.
            old_value: Replace the key value if it is the old value.
            new_value: The new value to replace the old value.

        Returns:
            ``True`` if the value was replaced, ``False`` otherwise.
        """
        check_not_none(key, "key can't be none")
        check_not_none(old_value, "old_value can't be none")
        check_not_none(new_value, "new_value can't be none")
        try:
            key_data = self._to_data(key)
            old_value_data = self._to_data(old_value)
            new_value_data = self._to_data(new_value)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.replace_if_same(key, old_value, new_value)

        request = transactional_map_replace_if_same_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data, old_value_data, new_value_data
        )
        return self._invoke(request, transactional_map_replace_if_same_codec.decode_response)

    def remove(self, key: KeyType) -> typing.Optional[ValueType]:
        """Transactional implementation of
        :func:`Map.remove(key) <hazelcast.proxy.map.Map.remove>`

        The object to be removed will be removed from only the current
        transaction context until the transaction is committed.

        Args:
            key: Key of the mapping to be deleted.

        Returns:
            The previous value associated with key, or ``None`` if there was no
            mapping for key.
        """
        check_not_none(key, "key can't be none")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.remove(key)

        def handler(message):
            return self._to_object(transactional_map_remove_codec.decode_response(message))

        request = transactional_map_remove_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data
        )
        return self._invoke(request, handler)

    def remove_if_same(self, key: KeyType, value: ValueType) -> bool:
        """Transactional implementation of :func:`Map.remove_if_same(key, value)
        <hazelcast.proxy.map.Map.remove_if_same>`

        The object to be removed will be removed from only the current
        transaction context until the transaction is committed.

        Args:
            key: The specified key.
            value: Remove the key if it has this value.

        Returns:
            ``True`` if the value was removed, ``False`` otherwise.
        """
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.remove_if_same(key, value)

        request = transactional_map_remove_if_same_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data, value_data
        )
        return self._invoke(request, transactional_map_remove_if_same_codec.decode_response)

    def delete(self, key: KeyType) -> None:
        """Transactional implementation of
        :func:`Map.delete(key) <hazelcast.proxy.map.Map.delete>`

        The object to be deleted will be removed from only the current
        transaction context until the transaction is committed.

        Args:
            key: Key of the mapping to be deleted.
        """
        check_not_none(key, "key can't be none")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.delete(key)

        request = transactional_map_delete_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data
        )
        return self._invoke(request)

    def key_set(self, predicate: Predicate = None) -> typing.List[KeyType]:
        """Transactional implementation of
        :func:`Map.key_set(predicate) <hazelcast.proxy.map.Map.key_set>`

        Args:
            predicate: Predicate to filter the entries.

        Returns:
            A list of the clone of the keys.
        """
        if predicate:
            try:
                predicate_data = self._to_data(predicate)
            except SchemaNotReplicatedError as e:
                self._send_schema(e)
                return self.key_set(predicate)

            def handler(message):
                return ImmutableLazyDataList(
                    transactional_map_key_set_with_predicate_codec.decode_response(message),
                    self._to_object,
                )

            request = transactional_map_key_set_with_predicate_codec.encode_request(
                self.name, self.transaction.id, thread_id(), predicate_data
            )
        else:

            def handler(message):
                return ImmutableLazyDataList(
                    transactional_map_key_set_codec.decode_response(message), self._to_object
                )

            request = transactional_map_key_set_codec.encode_request(
                self.name, self.transaction.id, thread_id()
            )

        return self._invoke(request, handler)

    def values(self, predicate: Predicate = None) -> typing.List[ValueType]:
        """Transactional implementation of
        :func:`Map.values(predicate) <hazelcast.proxy.map.Map.values>`

        Args:
            predicate: Predicate to filter the entries.

        Returns:
            A list of clone of the values contained in this map.
        """
        if predicate:
            try:
                predicate_data = self._to_data(predicate)
            except SchemaNotReplicatedError as e:
                self._send_schema(e)
                return self.values(predicate)

            def handler(message):
                return ImmutableLazyDataList(
                    transactional_map_values_with_predicate_codec.decode_response(message),
                    self._to_object,
                )

            request = transactional_map_values_with_predicate_codec.encode_request(
                self.name, self.transaction.id, thread_id(), predicate_data
            )
        else:

            def handler(message):
                return ImmutableLazyDataList(
                    transactional_map_values_codec.decode_response(message), self._to_object
                )

            request = transactional_map_values_codec.encode_request(
                self.name, self.transaction.id, thread_id()
            )

        return self._invoke(request, handler)
