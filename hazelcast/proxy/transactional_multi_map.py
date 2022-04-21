import typing

from hazelcast.protocol.codec import (
    transactional_multi_map_get_codec,
    transactional_multi_map_put_codec,
    transactional_multi_map_remove_codec,
    transactional_multi_map_remove_entry_codec,
    transactional_multi_map_size_codec,
    transactional_multi_map_value_count_codec,
)
from hazelcast.proxy.base import TransactionalProxy
from hazelcast.types import KeyType, ValueType
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import check_not_none, thread_id, ImmutableLazyDataList


class TransactionalMultiMap(TransactionalProxy, typing.Generic[KeyType, ValueType]):
    """Transactional implementation of
    :class:`~hazelcast.proxy.multi_map.MultiMap`.
    """

    def put(self, key: KeyType, value: ValueType) -> bool:
        """Transactional implementation of
        :func:`MultiMap.put(key, value)
        <hazelcast.proxy.multi_map.MultiMap.put>`

        Args:
            key: The key to be stored.
            value: The value to be stored.

        Returns:
            ``True`` if the size of the multimap is increased, ``False`` if
            the multimap already contains the key-value tuple.
        """
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.put(key, value)

        request = transactional_multi_map_put_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data, value_data
        )
        return self._invoke(request, transactional_multi_map_put_codec.decode_response)

    def get(self, key: KeyType) -> typing.Optional[typing.List[ValueType]]:
        """Transactional implementation of
        :func:`MultiMap.get(key) <hazelcast.proxy.multi_map.MultiMap.get>`

        Args:
            key: The key whose associated values are returned.

        Returns:
            The collection of the values associated with the key.
        """
        check_not_none(key, "key can't be none")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.get(key)

        def handler(message):
            return ImmutableLazyDataList(
                transactional_multi_map_get_codec.decode_response(message), self._to_object
            )

        request = transactional_multi_map_get_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data
        )
        return self._invoke(request, handler)

    def remove(self, key: KeyType, value: ValueType) -> bool:
        """Transactional implementation of :func:`MultiMap.remove(key, value)
        <hazelcast.proxy.multi_map.MultiMap.remove>`

        Args:
            key: The key of the entry to remove.
            value: The value of the entry to remove.

        Returns:
            ``True`` if the item is removed, ``False`` otherwise.
        """
        check_not_none(key, "key can't be none")
        check_not_none(value, "value can't be none")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.remove(key, value)

        request = transactional_multi_map_remove_entry_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data, value_data
        )
        return self._invoke(request, transactional_multi_map_remove_entry_codec.decode_response)

    def remove_all(self, key: KeyType) -> typing.List[ValueType]:
        """Transactional implementation of :func:`MultiMap.remove_all(key)
        <hazelcast.proxy.multi_map.MultiMap.remove_all>`

        Args:
            key: The key of the entries to remove.

        Returns:
            The collection of the values associated with the key.
        """
        check_not_none(key, "key can't be none")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.remove_all(key)

        def handler(message):
            return ImmutableLazyDataList(
                transactional_multi_map_remove_codec.decode_response(message), self._to_object
            )

        request = transactional_multi_map_remove_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data
        )
        return self._invoke(request, handler)

    def value_count(self, key: KeyType) -> int:
        """Transactional implementation of :func:`MultiMap.value_count(key)
        <hazelcast.proxy.multi_map.MultiMap.value_count>`

        Args:
            key: The key whose number of values is to be returned.

        Returns:
            The number of values matching the given key in the multimap.
        """
        check_not_none(key, "key can't be none")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.value_count(key)

        request = transactional_multi_map_value_count_codec.encode_request(
            self.name, self.transaction.id, thread_id(), key_data
        )
        return self._invoke(request, transactional_multi_map_value_count_codec.decode_response)

    def size(self) -> int:
        """Transactional implementation of
        :func:`MultiMap.size() <hazelcast.proxy.multi_map.MultiMap.size>`

        Returns:
            The number of key-value tuples in the multimap.
        """
        request = transactional_multi_map_size_codec.encode_request(
            self.name, self.transaction.id, thread_id()
        )
        return self._invoke(request, transactional_multi_map_size_codec.decode_response)
