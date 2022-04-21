import typing

from hazelcast.protocol.codec import (
    transactional_set_add_codec,
    transactional_set_remove_codec,
    transactional_set_size_codec,
)
from hazelcast.proxy.base import TransactionalProxy
from hazelcast.types import ItemType
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import check_not_none, thread_id


class TransactionalSet(TransactionalProxy, typing.Generic[ItemType]):
    """Transactional implementation of :class:`~hazelcast.proxy.set.Set`."""

    def add(self, item: ItemType) -> bool:
        """Transactional implementation of
        :func:`Set.add(item) <hazelcast.proxy.set.Set.add>`

        Args:
            item: The new item to be added.

        Returns:
            ``True`` if item is added successfully, ``False`` otherwise.
        """
        check_not_none(item, "item can't be none")
        try:
            item_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.add(item)

        request = transactional_set_add_codec.encode_request(
            self.name, self.transaction.id, thread_id(), item_data
        )
        return self._invoke(request, transactional_set_add_codec.decode_response)

    def remove(self, item: ItemType) -> bool:
        """Transactional implementation of
        :func:`Set.remove(item) <hazelcast.proxy.set.Set.remove>`

        Args:
            item: The specified item to be deleted.

        Returns:
            ``True`` if item is remove successfully, ``False`` otherwise.
        """
        check_not_none(item, "item can't be none")
        try:
            item_data = self._to_data(item)
        except SchemaNotReplicatedError as e:
            self._send_schema(e)
            return self.remove(item)

        request = transactional_set_remove_codec.encode_request(
            self.name, self.transaction.id, thread_id(), item_data
        )
        return self._invoke(request, transactional_set_remove_codec.decode_response)

    def size(self) -> int:
        """Transactional implementation of
        :func:`Set.size() <hazelcast.proxy.set.Set.size>`

        Returns:
            Size of the set.
        """
        request = transactional_set_size_codec.encode_request(
            self.name, self.transaction.id, thread_id()
        )
        return self._invoke(request, transactional_set_size_codec.decode_response)
